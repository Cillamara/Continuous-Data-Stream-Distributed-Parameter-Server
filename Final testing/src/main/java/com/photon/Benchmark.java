package com.photon;

import com.photon.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

/**
 * Standalone performance benchmark for the Photon pipeline.
 *
 * Usage:
 *   1. Start infrastructure:  docker compose up -d
 *   2. Run benchmark:         ./gradlew run -PmainClass=com.photon.Benchmark
 *
 * Measures:
 *   - IdRegistry throughput & latency (sequential + concurrent)
 *   - EventStore (Redis) read/write throughput
 *   - End-to-end Joiner throughput & latency
 *   - Exactly-once dedup accuracy
 *   - HLC/EventId generation speed
 */
public class Benchmark {

    // Ports for benchmark servers (avoid clashing with any running services)
    static final int REGISTRY_PORT = 18090;
    static final int JOINER_PORT   = 18092;
    static final String OUTPUT_TOPIC = "photon.joined.bench";

    public static void main(String[] args) throws Exception {

        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("  PHOTON PIPELINE — PERFORMANCE BENCHMARK");
        System.out.println("=".repeat(70));
        System.out.println("  Infrastructure: docker compose (localhost)");
        System.out.println("  etcd=localhost:2379  redis=localhost:6379  kafka=localhost:9092");
        System.out.println("=".repeat(70));
        System.out.println();

        // ── Start servers ────────────────────────────────────────────────
        IDRegistryServer registryServer = new IDRegistryServer(
                "http://localhost:2379", REGISTRY_PORT, REGISTRY_PORT + 1, 1);
        registryServer.start();
        System.out.println("  IdRegistry server started on port " + REGISTRY_PORT);

        IDRegistryClient registryClient = new IDRegistryClient("localhost", REGISTRY_PORT);

        // Quick sanity check
        EventId test = EventId.generate();
        var testResult = registryClient.register(test.value, test.hlcTimestamp);
        System.out.println("  IdRegistry sanity check: " + testResult);

        CacheEventStore  cache          = new CacheEventStore("localhost", 6379, 300);

        EventStore noopLogs = new EventStore() {
            public Optional<byte[]> lookup(String q) { return Optional.empty(); }
            public void store(String q, byte[] b) {}
            public void close() {}
        };
        TieredEventStore eventStore = new TieredEventStore(cache, noopLogs);

        KafkaProducer<String, byte[]> producer = buildProducer("localhost:9092");
        Joiner joiner = new Joiner(eventStore, registryClient, producer,
                OUTPUT_TOPIC, JoinAdapter.defaultAdapter(),
                JOINER_PORT, JOINER_PORT + 1, 5000);
        joiner.start();

        Thread.sleep(1000); // let servers warm up

        try {
            benchHlcEventId();
            benchEventStoreRedis(cache);
            benchIdRegistrySequential(registryClient);
            benchIdRegistryConcurrent();
            benchDedupAccuracy();
            benchJoinerSequential(cache);
            benchJoinerConcurrent(cache);
        } finally {
            joiner.shutdown();
            registryServer.shutdown();
            cache.close();
            registryClient.close();
            producer.close();
        }

        System.out.println("\nBenchmark complete.");
        System.exit(0);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  1. HLC + EventId generation
    // ══════════════════════════════════════════════════════════════════════
    static void benchHlcEventId() {
        int count = 100_000;
        HybridLogicalClock hlc = new HybridLogicalClock();

        long t0 = System.nanoTime();
        for (int i = 0; i < count; i++) hlc.now();
        long hlcNs = System.nanoTime() - t0;

        Set<Long> ids = new HashSet<>(count);
        long t1 = System.nanoTime();
        for (int i = 0; i < count; i++) ids.add(EventId.generate().value);
        long eidNs = System.nanoTime() - t1;

        header("1. HLC + EventId Generation (" + count + " ops)");
        metric("HLC throughput", fmt(count / (hlcNs / 1e9)) + " ops/sec");
        metric("HLC avg latency", fmtNs(hlcNs / count));
        metric("EventId throughput", fmt(count / (eidNs / 1e9)) + " ops/sec");
        metric("EventId avg latency", fmtNs(eidNs / count));
        metric("Unique IDs", ids.size() + "/" + count + (ids.size() == count ? " (PASS)" : " (FAIL)"));
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  2. EventStore (Redis) read/write
    // ══════════════════════════════════════════════════════════════════════
    static void benchEventStoreRedis(CacheEventStore cache) {
        int count = 5000;
        byte[] payload = new byte[256];
        new Random(42).nextBytes(payload);

        // Writes
        long[] wLat = new long[count];
        long wStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            long t = System.nanoTime();
            cache.store("bench-q-" + i, payload);
            wLat[i] = System.nanoTime() - t;
        }
        long wTotal = System.nanoTime() - wStart;

        // Reads (hits)
        long[] rLat = new long[count];
        long rStart = System.nanoTime();
        int hits = 0;
        for (int i = 0; i < count; i++) {
            long t = System.nanoTime();
            if (cache.lookup("bench-q-" + i).isPresent()) hits++;
            rLat[i] = System.nanoTime() - t;
        }
        long rTotal = System.nanoTime() - rStart;

        // Reads (misses)
        long[] mLat = new long[count];
        long mStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            long t = System.nanoTime();
            cache.lookup("miss-q-" + i);
            mLat[i] = System.nanoTime() - t;
        }
        long mTotal = System.nanoTime() - mStart;

        Arrays.sort(wLat); Arrays.sort(rLat); Arrays.sort(mLat);

        header("2. EventStore — Redis (256-byte payload, " + count + " ops)");
        System.out.println("  WRITES:");
        metric("  Throughput", fmt(count / (wTotal / 1e9)) + " writes/sec");
        latencyBlock(wLat);
        System.out.println("  READS (cache hit):");
        metric("  Hit rate", hits + "/" + count);
        metric("  Throughput", fmt(count / (rTotal / 1e9)) + " reads/sec");
        latencyBlock(rLat);
        System.out.println("  READS (cache miss):");
        metric("  Throughput", fmt(count / (mTotal / 1e9)) + " reads/sec");
        latencyBlock(mLat);
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  3. IdRegistry — sequential inserts
    // ══════════════════════════════════════════════════════════════════════
    static void benchIdRegistrySequential(IDRegistryClient client) {
        int count = 1000;
        // warmup
        for (int i = 0; i < 50; i++) client.register(EventId.generate().value, 0L);

        long[] lat = new long[count];
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            EventId eid = EventId.generate();
            long t = System.nanoTime();
            client.register(eid.value, eid.hlcTimestamp);
            lat[i] = System.nanoTime() - t;
        }
        long elapsed = System.nanoTime() - start;
        Arrays.sort(lat);

        header("3. IdRegistry — Sequential Insert (" + count + " events)");
        metric("Throughput", fmt(count / (elapsed / 1e9)) + " inserts/sec");
        latencyBlock(lat);
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  4. IdRegistry — concurrent inserts
    // ══════════════════════════════════════════════════════════════════════
    static void benchIdRegistryConcurrent() throws Exception {
        int total = 2000;
        int threads = 8;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        ConcurrentLinkedQueue<Long> allLat = new ConcurrentLinkedQueue<>();
        AtomicInteger ok = new AtomicInteger();

        int perThread = total / threads;
        long start = System.nanoTime();
        List<Future<?>> futs = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futs.add(pool.submit(() -> {
                IDRegistryClient c = new IDRegistryClient("localhost", REGISTRY_PORT);
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                try {
                    for (int i = 0; i < perThread; i++) {
                        EventId eid = EventId.generate();
                        long t0 = System.nanoTime();
                        var s = c.register(eid.value, eid.hlcTimestamp);
                        allLat.add(System.nanoTime() - t0);
                        if (s == InsertResponse.Status.SUCCESS) ok.incrementAndGet();
                    }
                } finally { c.close(); }
            }));
        }
        for (var f : futs) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();

        long[] sorted = allLat.stream().mapToLong(Long::longValue).sorted().toArray();

        header("4. IdRegistry — Concurrent Insert (" + total + " events, " + threads + " threads)");
        metric("Throughput", fmt(total / (elapsed / 1e9)) + " inserts/sec");
        metric("Successful", ok.get() + "/" + total);
        latencyBlock(sorted);
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  5. Dedup accuracy — same event sent from N clients
    // ══════════════════════════════════════════════════════════════════════
    static void benchDedupAccuracy() throws Exception {
        int unique = 200;
        int copies = 5;
        int threads = 4;

        EventId[] events = new EventId[unique];
        for (int i = 0; i < unique; i++) events[i] = EventId.generate();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicInteger success = new AtomicInteger(), dup = new AtomicInteger(), retry = new AtomicInteger();

        List<Future<?>> futs = new ArrayList<>();
        for (int d = 0; d < copies; d++) {
            futs.add(pool.submit(() -> {
                IDRegistryClient c = new IDRegistryClient("localhost", REGISTRY_PORT);
                try {
                    for (EventId eid : events) {
                        var s = c.register(eid.value, eid.hlcTimestamp);
                        switch (s) {
                            case SUCCESS -> success.incrementAndGet();
                            case ALREADY_EXISTS -> dup.incrementAndGet();
                            case RETRY -> retry.incrementAndGet();
                            default -> {}
                        }
                    }
                } finally { c.close(); }
            }));
        }
        for (var f : futs) f.get();
        pool.shutdown();

        int totalAttempts = unique * copies;
        header("5. Exactly-Once Dedup (" + unique + " events x " + copies + " copies)");
        metric("SUCCESS (first wins)", String.valueOf(success.get()));
        metric("ALREADY_EXISTS (deduped)", String.valueOf(dup.get()));
        metric("RETRY (same-token)", String.valueOf(retry.get()));
        metric("Dedup rate", String.format("%.1f%%", dup.get() * 100.0 / totalAttempts));
        metric("Correctness", success.get() <= unique ? "PASS" : "FAIL");
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  6. Joiner — sequential end-to-end
    // ══════════════════════════════════════════════════════════════════════
    static void benchJoinerSequential(CacheEventStore cache) {
        int count = 500;
        byte[] qPayload = "advertiser=bench&kw=seq".getBytes(StandardCharsets.UTF_8);
        byte[] cPayload = "pos=1&cost=0.10".getBytes(StandardCharsets.UTF_8);

        String[] qids = new String[count];
        EventId[] cids = new EventId[count];
        for (int i = 0; i < count; i++) {
            qids[i] = "seq-q-" + UUID.randomUUID();
            cids[i] = EventId.generate();
            cache.store(qids[i], qPayload);
        }

        long[] lat = new long[count];
        int joined = 0;
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            long t = System.nanoTime();
            var s = callJoiner(cids[i], qids[i], cPayload, JOINER_PORT);
            lat[i] = System.nanoTime() - t;
            if (s == JoinResponse.Status.JOINED) joined++;
        }
        long elapsed = System.nanoTime() - start;
        Arrays.sort(lat);

        header("6. Joiner — Sequential (" + count + " joins)");
        metric("Joined", joined + "/" + count);
        metric("Throughput", fmt(count / (elapsed / 1e9)) + " joins/sec");
        latencyBlock(lat);
        footer();
    }

    // ══════════════════════════════════════════════════════════════════════
    //  7. Joiner — concurrent end-to-end
    // ══════════════════════════════════════════════════════════════════════
    static void benchJoinerConcurrent(CacheEventStore cache) throws Exception {
        int total = 1000;
        int threads = 8;
        byte[] qPayload = "advertiser=bench&kw=conc".getBytes(StandardCharsets.UTF_8);
        byte[] cPayload = "pos=2&cost=0.20".getBytes(StandardCharsets.UTF_8);

        String[] qids = new String[total];
        EventId[] cids = new EventId[total];
        for (int i = 0; i < total; i++) {
            qids[i] = "conc-q-" + UUID.randomUUID();
            cids[i] = EventId.generate();
            cache.store(qids[i], qPayload);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger joined = new AtomicInteger();
        ConcurrentLinkedQueue<Long> allLat = new ConcurrentLinkedQueue<>();

        int perThread = total / threads;
        long start = System.nanoTime();
        List<Future<?>> futs = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int off = t * perThread;
            futs.add(pool.submit(() -> {
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                for (int i = 0; i < perThread; i++) {
                    int idx = off + i;
                    long t0 = System.nanoTime();
                    var s = callJoiner(cids[idx], qids[idx], cPayload, JOINER_PORT);
                    allLat.add(System.nanoTime() - t0);
                    if (s == JoinResponse.Status.JOINED) joined.incrementAndGet();
                }
            }));
        }
        for (var f : futs) f.get();
        long elapsed = System.nanoTime() - start;
        pool.shutdown();

        long[] sorted = allLat.stream().mapToLong(Long::longValue).sorted().toArray();

        header("7. Joiner — Concurrent (" + total + " joins, " + threads + " threads)");
        metric("Joined", joined.get() + "/" + total);
        metric("Throughput", fmt(total / (elapsed / 1e9)) + " joins/sec");
        latencyBlock(sorted);
        footer();
    }

    // ── gRPC call helper ─────────────────────────────────────────────────
    static JoinResponse.Status callJoiner(EventId clickId, String queryId, byte[] payload, int port) {
        ManagedChannel ch = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext().build();
        try {
            JoinerServiceGrpc.JoinerServiceBlockingStub stub =
                    JoinerServiceGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(10, TimeUnit.SECONDS);
            JoinRequest req = JoinRequest.newBuilder()
                    .setClickEventId(clickId.value)
                    .setClickHlc(clickId.hlcTimestamp)
                    .setQueryId(queryId)
                    .setClickPayload(com.google.protobuf.ByteString.copyFrom(payload))
                    .build();
            return stub.join(req).getStatus();
        } finally {
            ch.shutdownNow();
        }
    }

    static KafkaProducer<String, byte[]> buildProducer(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    // ── Pretty-print ─────────────────────────────────────────────────────
    static void header(String t) {
        System.out.println();
        System.out.println("┌─" + "─".repeat(t.length()) + "─┐");
        System.out.println("│ " + t + " │");
        System.out.println("└─" + "─".repeat(t.length()) + "─┘");
    }
    static void footer() { System.out.println("─".repeat(60)); }
    static void metric(String k, String v) { System.out.printf("  %-30s %s%n", k + ":", v); }
    static void latencyBlock(long[] sorted) {
        metric("  p50", fmtNs(pct(sorted, 50)));
        metric("  p95", fmtNs(pct(sorted, 95)));
        metric("  p99", fmtNs(pct(sorted, 99)));
        metric("  max", fmtNs(sorted[sorted.length - 1]));
    }
    static long pct(long[] s, int p) {
        return s[Math.min((int) Math.ceil(p / 100.0 * s.length) - 1, s.length - 1)];
    }
    static String fmtNs(long ns) {
        if (ns < 1_000) return ns + " ns";
        if (ns < 1_000_000) return String.format("%.1f µs", ns / 1e3);
        if (ns < 1_000_000_000) return String.format("%.2f ms", ns / 1e6);
        return String.format("%.2f sec", ns / 1e9);
    }
    static String fmt(double v) { return String.format("%.0f", v); }
}
