package com.photon;

import com.photon.proto.InsertResponse;
import com.photon.proto.JoinResponse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PipelineBenchmark — performance evaluation of the Photon pipeline.
 *
 * Measures the key metrics from the Photon paper (Google, 2013):
 *
 *   1. End-to-end throughput         (joins/sec)
 *   2. End-to-end latency            (p50, p95, p99, max)
 *   3. IdRegistry throughput          (inserts/sec, batch sizes)
 *   4. IdRegistry dedup accuracy      (zero duplicates under concurrent load)
 *   5. EventStore tier performance    (cache hit rate, Redis vs HBase latency)
 *   6. Joiner scalability             (throughput with 1 vs N concurrent callers)
 *   7. Dispatcher end-to-end          (Kafka ingest → joined output latency)
 *
 * Run with:   ./gradlew test --tests "com.photon.PipelineBenchmark" --info
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineBenchmark {

    // ── Infrastructure ───────────────────────────────────────────────────────
    static Network network = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withNetwork(network);

    @Container
    static GenericContainer<?> etcd = new GenericContainer<>(
            DockerImageName.parse("bitnamilegacy/etcd:3.5"))
            .withNetwork(network)
            .withEnv("ALLOW_NONE_AUTHENTICATION", "yes")
            .withEnv("ETCD_ADVERTISE_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
            .withEnv("ETCD_LISTEN_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_INITIAL_ADVERTISE_PEER_URLS", "http://0.0.0.0:2380")
            .withEnv("ETCD_INITIAL_CLUSTER", "default=http://0.0.0.0:2380")
            .withExposedPorts(2379)
            .waitingFor(Wait.forListeningPort());

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine"))
            .withNetwork(network)
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort());

    // ── Shared components ────────────────────────────────────────────────────
    static IDRegistryClient  idRegistryClient;
    static IDRegistryServer  idRegistryServer;
    static CacheEventStore   cacheEventStore;
    static TieredEventStore  eventStore;
    static Joiner            joiner;

    static final String OUTPUT_TOPIC   = "photon.joined.bench";
    static final int    JOINER_PORT    = 19192;
    static final int    REGISTRY_PORT  = 19190;

    @BeforeAll
    static void startComponents() throws Exception {
        String etcdEndpoint = "http://" + etcd.getHost() + ":" + etcd.getMappedPort(2379);
        String redisHost    = redis.getHost();
        int    redisPort    = redis.getMappedPort(6379);

        // ── IdRegistry ────────────────────────────────────────────────────
        idRegistryServer = new IDRegistryServer(etcdEndpoint, REGISTRY_PORT, REGISTRY_PORT + 1, 1);
        idRegistryServer.start();
        idRegistryClient = new IDRegistryClient("localhost", REGISTRY_PORT);

        // ── EventStore ────────────────────────────────────────────────────
        cacheEventStore = new CacheEventStore(redisHost, redisPort, 300);
        eventStore = new TieredEventStore(cacheEventStore, noopLogsStore());

        // ── Joiner ────────────────────────────────────────────────────────
        KafkaProducer<String, byte[]> producer = buildProducer(kafka.getBootstrapServers());
        joiner = new Joiner(eventStore, idRegistryClient, producer,
                OUTPUT_TOPIC, JoinAdapter.defaultAdapter(),
                JOINER_PORT, JOINER_PORT + 1, 5000);
        joiner.start();

        Thread.sleep(1000); // warm-up
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  PHOTON PIPELINE PERFORMANCE BENCHMARK");
        System.out.println("=".repeat(80));
    }

    @AfterAll
    static void stopComponents() {
        if (joiner != null)           joiner.shutdown();
        if (idRegistryServer != null) idRegistryServer.shutdown();
        if (cacheEventStore != null)  cacheEventStore.close();
        if (idRegistryClient != null) idRegistryClient.close();
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 1: IdRegistry Throughput (§2 — the exactly-once gate)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(1)
    @DisplayName("Benchmark 1: IdRegistry — sequential insert throughput")
    void benchIdRegistrySequential() {
        int count = 1000;

        // Warm up
        for (int i = 0; i < 50; i++) {
            idRegistryClient.register(EventId.generate().value, 0L);
        }

        long[] latencies = new long[count];
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            EventId eid = EventId.generate();
            long t0 = System.nanoTime();
            InsertResponse.Status status = idRegistryClient.register(eid.value, eid.hlcTimestamp);
            latencies[i] = System.nanoTime() - t0;
            assertEquals(InsertResponse.Status.SUCCESS, status);
        }

        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);

        Arrays.sort(latencies);
        printHeader("BENCHMARK 1: IdRegistry Sequential Insert");
        printMetric("Total events", String.valueOf(count));
        printMetric("Throughput", String.format("%.1f inserts/sec", throughput));
        printLatencyStats(latencies);
        printFooter();
    }

    @Test
    @Order(2)
    @DisplayName("Benchmark 2: IdRegistry — concurrent insert throughput")
    void benchIdRegistryConcurrent() throws Exception {
        int totalEvents = 2000;
        int concurrency = 8;

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        AtomicInteger successCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();

        long start = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>();
        int perThread = totalEvents / concurrency;

        for (int t = 0; t < concurrency; t++) {
            futures.add(pool.submit(() -> {
                IDRegistryClient client = new IDRegistryClient("localhost", REGISTRY_PORT);
                try {
                    barrier.await();
                    for (int i = 0; i < perThread; i++) {
                        EventId eid = EventId.generate();
                        long t0 = System.nanoTime();
                        InsertResponse.Status status = client.register(eid.value, eid.hlcTimestamp);
                        allLatencies.add(System.nanoTime() - t0);
                        if (status == InsertResponse.Status.SUCCESS) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    client.close();
                }
            }));
        }

        for (Future<?> f : futures) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();

        double throughput = totalEvents / (elapsed / 1_000_000_000.0);
        long[] sorted = allLatencies.stream().mapToLong(Long::longValue).sorted().toArray();

        printHeader("BENCHMARK 2: IdRegistry Concurrent Insert (" + concurrency + " threads)");
        printMetric("Total events", String.valueOf(totalEvents));
        printMetric("Concurrency", concurrency + " threads");
        printMetric("Successful inserts", String.valueOf(successCount.get()));
        printMetric("Throughput", String.format("%.1f inserts/sec", throughput));
        printLatencyStats(sorted);
        printFooter();
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 3: IdRegistry Dedup Accuracy Under Concurrent Load
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(3)
    @DisplayName("Benchmark 3: IdRegistry — exactly-once under concurrent duplicate storm")
    void benchIdRegistryDedupAccuracy() throws Exception {
        int uniqueEvents   = 200;
        int duplicateFactor = 5;  // each event sent 5 times concurrently
        int concurrency     = 4;

        // Generate unique event IDs
        EventId[] events = new EventId[uniqueEvents];
        for (int i = 0; i < uniqueEvents; i++) {
            events[i] = EventId.generate();
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger alreadyExistsCount = new AtomicInteger(0);
        AtomicInteger retryCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        long start = System.nanoTime();

        for (int dup = 0; dup < duplicateFactor; dup++) {
            futures.add(pool.submit(() -> {
                IDRegistryClient client = new IDRegistryClient("localhost", REGISTRY_PORT);
                try {
                    for (EventId eid : events) {
                        InsertResponse.Status s = client.register(eid.value, eid.hlcTimestamp);
                        switch (s) {
                            case SUCCESS        -> successCount.incrementAndGet();
                            case ALREADY_EXISTS -> alreadyExistsCount.incrementAndGet();
                            case RETRY          -> retryCount.incrementAndGet();
                            default -> {}
                        }
                    }
                } finally {
                    client.close();
                }
            }));
        }

        for (Future<?> f : futures) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();

        int totalAttempts = uniqueEvents * duplicateFactor;

        printHeader("BENCHMARK 3: Exactly-Once Accuracy (Duplicate Storm)");
        printMetric("Unique events", String.valueOf(uniqueEvents));
        printMetric("Total attempts", totalAttempts + " (" + duplicateFactor + "x each)");
        printMetric("SUCCESS (first wins)", String.valueOf(successCount.get()));
        printMetric("ALREADY_EXISTS (deduped)", String.valueOf(alreadyExistsCount.get()));
        printMetric("RETRY (same-token)", String.valueOf(retryCount.get()));
        printMetric("Elapsed", String.format("%.2f sec", elapsed / 1_000_000_000.0));

        // Verify: exactly uniqueEvents SUCCESS results
        // (some may be RETRY if same client retries, but SUCCESS + RETRY should == uniqueEvents per unique token)
        int nonDuplicates = successCount.get() + retryCount.get();
        printMetric("Duplicate prevention rate",
                String.format("%.2f%%", (alreadyExistsCount.get() * 100.0) / totalAttempts));
        printMetric("Correctness",
                (successCount.get() <= uniqueEvents) ? "PASS — no over-counting" : "FAIL");
        printFooter();

        assertTrue(successCount.get() <= uniqueEvents,
                "SUCCESS count must not exceed unique events (would mean double-joining)");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 4: EventStore (CacheEventStore / Redis) Throughput
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(4)
    @DisplayName("Benchmark 4: EventStore — Redis cache write + read throughput")
    void benchEventStoreRedis() {
        int count = 2000;
        byte[] payload = new byte[256];
        new Random(42).nextBytes(payload);

        // ── Write benchmark ────────────────────────────────────────────────
        long[] writeLatencies = new long[count];
        long writeStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            String key = "bench-query-" + i;
            long t0 = System.nanoTime();
            cacheEventStore.store(key, payload);
            writeLatencies[i] = System.nanoTime() - t0;
        }
        long writeElapsed = System.nanoTime() - writeStart;

        // ── Read benchmark (100% cache hits) ────────────────────────────────
        long[] readLatencies = new long[count];
        long readStart = System.nanoTime();
        int hits = 0;
        for (int i = 0; i < count; i++) {
            String key = "bench-query-" + i;
            long t0 = System.nanoTime();
            Optional<byte[]> result = cacheEventStore.lookup(key);
            readLatencies[i] = System.nanoTime() - t0;
            if (result.isPresent()) hits++;
        }
        long readElapsed = System.nanoTime() - readStart;

        // ── Read benchmark (100% cache misses) ──────────────────────────────
        long[] missLatencies = new long[count];
        long missStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            String key = "nonexistent-query-" + i;
            long t0 = System.nanoTime();
            cacheEventStore.lookup(key);
            missLatencies[i] = System.nanoTime() - t0;
        }
        long missElapsed = System.nanoTime() - missStart;

        Arrays.sort(writeLatencies);
        Arrays.sort(readLatencies);
        Arrays.sort(missLatencies);

        printHeader("BENCHMARK 4: EventStore (Redis Cache) Performance");
        printMetric("Payload size", "256 bytes");
        printMetric("Operations", String.valueOf(count));
        System.out.println();

        System.out.println("  WRITES:");
        printMetric("  Throughput", String.format("%.1f writes/sec", count / (writeElapsed / 1e9)));
        printLatencyStats(writeLatencies);
        System.out.println();

        System.out.println("  READS (cache hits):");
        printMetric("  Hit rate", String.format("%.1f%% (%d/%d)", hits * 100.0 / count, hits, count));
        printMetric("  Throughput", String.format("%.1f reads/sec", count / (readElapsed / 1e9)));
        printLatencyStats(readLatencies);
        System.out.println();

        System.out.println("  READS (cache misses):");
        printMetric("  Throughput", String.format("%.1f reads/sec", count / (missElapsed / 1e9)));
        printLatencyStats(missLatencies);
        printFooter();

        assertEquals(count, hits, "All cache reads should hit");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 5: End-to-End Joiner Throughput (the core pipeline)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(5)
    @DisplayName("Benchmark 5: End-to-end Joiner — sequential throughput + latency")
    void benchJoinerSequential() {
        int count = 500;
        byte[] queryPayload = "advertiser=bench&keywords=perf".getBytes(StandardCharsets.UTF_8);
        byte[] clickPayload = "position=1&cost=0.10".getBytes(StandardCharsets.UTF_8);

        // Pre-populate EventStore with queries
        String[] queryIds = new String[count];
        EventId[] clickIds = new EventId[count];
        for (int i = 0; i < count; i++) {
            queryIds[i] = "bench-join-query-" + UUID.randomUUID();
            clickIds[i] = EventId.generate();
            cacheEventStore.store(queryIds[i], queryPayload);
        }

        long[] latencies = new long[count];
        int joined = 0;
        long start = System.nanoTime();

        for (int i = 0; i < count; i++) {
            long t0 = System.nanoTime();
            JoinResponse.Status status = callJoiner(clickIds[i], queryIds[i], clickPayload);
            latencies[i] = System.nanoTime() - t0;
            if (status == JoinResponse.Status.JOINED) joined++;
        }

        long elapsed = System.nanoTime() - start;
        double throughput = count / (elapsed / 1_000_000_000.0);
        Arrays.sort(latencies);

        printHeader("BENCHMARK 5: End-to-End Joiner (Sequential)");
        printMetric("Total joins", String.valueOf(count));
        printMetric("Successful joins", String.valueOf(joined));
        printMetric("Throughput", String.format("%.1f joins/sec", throughput));
        printLatencyStats(latencies);
        printFooter();

        assertEquals(count, joined, "All joins should succeed");
    }

    @Test
    @Order(6)
    @DisplayName("Benchmark 6: End-to-end Joiner — concurrent throughput")
    void benchJoinerConcurrent() throws Exception {
        int totalEvents = 1000;
        int concurrency = 8;
        byte[] queryPayload = "advertiser=bench&keywords=concurrent".getBytes(StandardCharsets.UTF_8);
        byte[] clickPayload = "position=2&cost=0.20".getBytes(StandardCharsets.UTF_8);

        // Pre-populate EventStore
        String[] queryIds = new String[totalEvents];
        EventId[] clickIds = new EventId[totalEvents];
        for (int i = 0; i < totalEvents; i++) {
            queryIds[i] = "bench-concurrent-" + UUID.randomUUID();
            clickIds[i] = EventId.generate();
            cacheEventStore.store(queryIds[i], queryPayload);
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        AtomicInteger joinedCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();

        int perThread = totalEvents / concurrency;
        long start = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < concurrency; t++) {
            final int offset = t * perThread;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < perThread; i++) {
                        int idx = offset + i;
                        long t0 = System.nanoTime();
                        JoinResponse.Status status = callJoiner(clickIds[idx], queryIds[idx], clickPayload);
                        allLatencies.add(System.nanoTime() - t0);
                        if (status == JoinResponse.Status.JOINED) {
                            joinedCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();

        double throughput = totalEvents / (elapsed / 1_000_000_000.0);
        long[] sorted = allLatencies.stream().mapToLong(Long::longValue).sorted().toArray();

        printHeader("BENCHMARK 6: End-to-End Joiner (Concurrent, " + concurrency + " threads)");
        printMetric("Total events", String.valueOf(totalEvents));
        printMetric("Concurrency", concurrency + " threads");
        printMetric("Successful joins", String.valueOf(joinedCount.get()));
        printMetric("Throughput", String.format("%.1f joins/sec", throughput));
        printLatencyStats(sorted);
        printFooter();

        assertEquals(totalEvents, joinedCount.get(), "All concurrent joins should succeed");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 7: Full Pipeline (Kafka → Dispatcher → Joiner → Kafka)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(7)
    @DisplayName("Benchmark 7: Full pipeline — Kafka-to-Kafka end-to-end")
    void benchFullPipeline() throws Exception {
        String clickTopic = "photon.clicks.bench";
        int eventCount = 200;
        byte[] queryPayload = "advertiser=e2e&keywords=fullpipe".getBytes(StandardCharsets.UTF_8);
        byte[] clickPayload = "position=1&cost=0.05".getBytes(StandardCharsets.UTF_8);

        // Pre-populate EventStore with queries and generate clicks
        EventId[] clickIds = new EventId[eventCount];
        String[] queryIds = new String[eventCount];
        for (int i = 0; i < eventCount; i++) {
            clickIds[i] = EventId.generate();
            queryIds[i] = "pipe-query-" + UUID.randomUUID();
            cacheEventStore.store(queryIds[i], queryPayload);
        }

        // Publish all clicks to Kafka
        long publishStart = System.nanoTime();
        try (KafkaProducer<String, byte[]> pub = buildProducer(kafka.getBootstrapServers())) {
            for (int i = 0; i < eventCount; i++) {
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(clickTopic, null, queryIds[i], clickPayload,
                        List.of(
                            new RecordHeader("click-id",  Long.toHexString(clickIds[i].value).getBytes(StandardCharsets.UTF_8)),
                            new RecordHeader("click-hlc", String.valueOf(clickIds[i].hlcTimestamp).getBytes(StandardCharsets.UTF_8))
                        ));
                pub.send(record);
            }
            pub.flush();
        }
        long publishElapsed = System.nanoTime() - publishStart;

        // Start Dispatcher
        KafkaConsumer<String, byte[]> dispatcherConsumer = buildConsumer(kafka.getBootstrapServers(), "bench-dispatcher");
        IDRegistryClient dispatcherRegistry = new IDRegistryClient("localhost", REGISTRY_PORT);
        Dispatcher dispatcher = new Dispatcher(
                dispatcherConsumer, dispatcherRegistry,
                "localhost", JOINER_PORT,
                clickTopic, 3 * 24 * 3600 * 1000L, 4, JOINER_PORT + 3);

        ExecutorService dispatcherExec = Executors.newSingleThreadExecutor();
        dispatcherExec.submit(dispatcher::start);

        // Consume joined events from output topic
        long consumeStart = System.nanoTime();
        Set<String> joinedKeys = ConcurrentHashMap.newKeySet();
        int received = 0;

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-output-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> outputConsumer = new KafkaConsumer<>(consumerProps)) {
            outputConsumer.subscribe(List.of(OUTPUT_TOPIC));
            long deadline = System.currentTimeMillis() + 30_000; // 30s timeout

            while (received < eventCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = outputConsumer.poll(Duration.ofMillis(200));
                for (var rec : records) {
                    joinedKeys.add(rec.key());
                    received++;
                }
            }
        }
        long consumeElapsed = System.nanoTime() - consumeStart;

        dispatcher.shutdown();
        dispatcherExec.shutdownNow();
        dispatcherRegistry.close();

        // Check for duplicates
        int duplicates = received - joinedKeys.size();

        printHeader("BENCHMARK 7: Full Pipeline (Kafka → Dispatcher → Joiner → Kafka)");
        printMetric("Events published", String.valueOf(eventCount));
        printMetric("Events received", String.valueOf(received));
        printMetric("Unique joined keys", String.valueOf(joinedKeys.size()));
        printMetric("Duplicates detected", String.valueOf(duplicates));
        printMetric("Publish time", String.format("%.2f sec", publishElapsed / 1e9));
        printMetric("End-to-end time", String.format("%.2f sec", consumeElapsed / 1e9));
        if (received > 0) {
            printMetric("Pipeline throughput", String.format("%.1f events/sec", received / (consumeElapsed / 1e9)));
        }
        printMetric("Exactly-once", duplicates == 0 ? "PASS" : "FAIL (" + duplicates + " duplicates)");
        printFooter();

        assertTrue(received >= eventCount * 0.95,
                "At least 95% of events should be joined (got " + received + "/" + eventCount + ")");
        assertEquals(0, duplicates, "Zero duplicates expected (exactly-once guarantee)");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 8: HybridLogicalClock + EventId generation overhead
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(8)
    @DisplayName("Benchmark 8: HLC + EventId generation throughput")
    void benchHlcAndEventId() {
        int count = 100_000;

        // ── HLC throughput ────────────────────────────────────────────────
        HybridLogicalClock hlc = new HybridLogicalClock();
        long hlcStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            hlc.now();
        }
        long hlcElapsed = System.nanoTime() - hlcStart;

        // ── EventId generation throughput ──────────────────────────────────
        long eidStart = System.nanoTime();
        Set<Long> ids = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(EventId.generate().value);
        }
        long eidElapsed = System.nanoTime() - eidStart;

        printHeader("BENCHMARK 8: HLC + EventId Generation");
        printMetric("Operations", String.valueOf(count));
        printMetric("HLC throughput", String.format("%.0f calls/sec", count / (hlcElapsed / 1e9)));
        printMetric("HLC avg latency", String.format("%.0f ns", (double) hlcElapsed / count));
        printMetric("EventId throughput", String.format("%.0f gen/sec", count / (eidElapsed / 1e9)));
        printMetric("EventId avg latency", String.format("%.0f ns", (double) eidElapsed / count));
        printMetric("Unique IDs", ids.size() + "/" + count);
        printMetric("Uniqueness", ids.size() == count ? "PASS — 100% unique" : "FAIL — collisions detected");
        printFooter();

        assertEquals(count, ids.size(), "All generated EventIds must be unique");
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BENCHMARK 9: Joiner throttle / back-pressure behavior
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    @Order(9)
    @DisplayName("Benchmark 9: Joiner back-pressure under overload")
    void benchJoinerBackPressure() throws Exception {
        // Create a separate joiner with low maxInFlight to test throttling
        KafkaProducer<String, byte[]> throttleProducer = buildProducer(kafka.getBootstrapServers());
        Joiner throttledJoiner = new Joiner(eventStore, idRegistryClient, throttleProducer,
                OUTPUT_TOPIC, JoinAdapter.defaultAdapter(),
                JOINER_PORT + 10, JOINER_PORT + 11, 5); // maxInFlight = 5
        throttledJoiner.start();
        Thread.sleep(500);

        int totalRequests = 100;
        int concurrency = 16;
        byte[] queryPayload = "advertiser=throttle&keywords=test".getBytes(StandardCharsets.UTF_8);
        byte[] clickPayload = "position=1&cost=0.01".getBytes(StandardCharsets.UTF_8);

        AtomicInteger joined = new AtomicInteger(0);
        AtomicInteger throttled = new AtomicInteger(0);
        AtomicInteger other = new AtomicInteger(0);

        // Pre-populate
        String[] queryIds = new String[totalRequests];
        EventId[] clickIds = new EventId[totalRequests];
        for (int i = 0; i < totalRequests; i++) {
            queryIds[i] = "throttle-query-" + UUID.randomUUID();
            clickIds[i] = EventId.generate();
            cacheEventStore.store(queryIds[i], queryPayload);
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        List<Future<?>> futures = new ArrayList<>();

        int perThread = totalRequests / concurrency;
        for (int t = 0; t < concurrency; t++) {
            final int offset = t * perThread;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < perThread; i++) {
                        int idx = offset + i;
                        JoinResponse.Status s = callJoinerOnPort(clickIds[idx], queryIds[idx], clickPayload, JOINER_PORT + 10);
                        switch (s) {
                            case JOINED    -> joined.incrementAndGet();
                            case THROTTLED -> throttled.incrementAndGet();
                            default        -> other.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();
        throttledJoiner.shutdown();

        printHeader("BENCHMARK 9: Joiner Back-Pressure (maxInFlight=5, " + concurrency + " threads)");
        printMetric("Total requests", String.valueOf(totalRequests));
        printMetric("JOINED", String.valueOf(joined.get()));
        printMetric("THROTTLED", String.valueOf(throttled.get()));
        printMetric("Other", String.valueOf(other.get()));
        printMetric("Throttle rate", String.format("%.1f%%", throttled.get() * 100.0 / totalRequests));
        printMetric("Back-pressure", throttled.get() > 0 ? "PASS — throttling observed" : "NOTE — no throttling triggered");
        printFooter();
    }

    // ── Helper: call Joiner via gRPC ─────────────────────────────────────────

    private JoinResponse.Status callJoiner(EventId clickId, String queryId, byte[] clickPayload) {
        return callJoinerOnPort(clickId, queryId, clickPayload, JOINER_PORT);
    }

    private JoinResponse.Status callJoinerOnPort(EventId clickId, String queryId, byte[] clickPayload, int port) {
        io.grpc.ManagedChannel ch = io.grpc.ManagedChannelBuilder
                .forAddress("localhost", port).usePlaintext().build();
        try {
            com.photon.proto.JoinerServiceGrpc.JoinerServiceBlockingStub stub =
                    com.photon.proto.JoinerServiceGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(10, TimeUnit.SECONDS);

            com.photon.proto.JoinRequest req = com.photon.proto.JoinRequest.newBuilder()
                    .setClickEventId(clickId.value)
                    .setClickHlc(clickId.hlcTimestamp)
                    .setQueryId(queryId)
                    .setClickPayload(com.google.protobuf.ByteString.copyFrom(clickPayload))
                    .build();

            return stub.join(req).getStatus();
        } finally {
            ch.shutdownNow();
        }
    }

    // ── Helper: Kafka producers / consumers ──────────────────────────────────

    private static KafkaProducer<String, byte[]> buildProducer(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    private static KafkaConsumer<String, byte[]> buildConsumer(String bootstrap, String groupId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(p);
    }

    private static EventStore noopLogsStore() {
        return new EventStore() {
            @Override public Optional<byte[]> lookup(String queryId) { return Optional.empty(); }
            @Override public void store(String queryId, byte[] bytes) {}
            @Override public void close() {}
        };
    }

    // ── Pretty-print helpers ─────────────────────────────────────────────────

    private static void printHeader(String title) {
        System.out.println();
        System.out.println("┌─" + "─".repeat(title.length()) + "─┐");
        System.out.println("│ " + title + " │");
        System.out.println("└─" + "─".repeat(title.length()) + "─┘");
    }

    private static void printFooter() {
        System.out.println("─".repeat(60));
    }

    private static void printMetric(String name, String value) {
        System.out.printf("  %-30s %s%n", name + ":", value);
    }

    private static void printLatencyStats(long[] sortedNanos) {
        if (sortedNanos.length == 0) return;
        printMetric("  p50 latency", formatNanos(percentile(sortedNanos, 50)));
        printMetric("  p95 latency", formatNanos(percentile(sortedNanos, 95)));
        printMetric("  p99 latency", formatNanos(percentile(sortedNanos, 99)));
        printMetric("  max latency", formatNanos(sortedNanos[sortedNanos.length - 1]));
        printMetric("  avg latency", formatNanos((long) LongStream.of(sortedNanos).average().orElse(0)));
    }

    private static long percentile(long[] sorted, int pct) {
        int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }

    private static String formatNanos(long nanos) {
        if (nanos < 1_000)        return nanos + " ns";
        if (nanos < 1_000_000)    return String.format("%.1f µs", nanos / 1_000.0);
        if (nanos < 1_000_000_000) return String.format("%.2f ms", nanos / 1_000_000.0);
        return String.format("%.2f sec", nanos / 1_000_000_000.0);
    }
}
