package com.photon.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.63.0)",
    comments = "Source: photon.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class IdRegistryServiceGrpc {

  private IdRegistryServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "photon.IdRegistryService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.photon.proto.LookupRequest,
      com.photon.proto.LookupResponse> getLookupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Lookup",
      requestType = com.photon.proto.LookupRequest.class,
      responseType = com.photon.proto.LookupResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.photon.proto.LookupRequest,
      com.photon.proto.LookupResponse> getLookupMethod() {
    io.grpc.MethodDescriptor<com.photon.proto.LookupRequest, com.photon.proto.LookupResponse> getLookupMethod;
    if ((getLookupMethod = IdRegistryServiceGrpc.getLookupMethod) == null) {
      synchronized (IdRegistryServiceGrpc.class) {
        if ((getLookupMethod = IdRegistryServiceGrpc.getLookupMethod) == null) {
          IdRegistryServiceGrpc.getLookupMethod = getLookupMethod =
              io.grpc.MethodDescriptor.<com.photon.proto.LookupRequest, com.photon.proto.LookupResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Lookup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.LookupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.LookupResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdRegistryServiceMethodDescriptorSupplier("Lookup"))
              .build();
        }
      }
    }
    return getLookupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.photon.proto.InsertRequest,
      com.photon.proto.InsertResponse> getInsertMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Insert",
      requestType = com.photon.proto.InsertRequest.class,
      responseType = com.photon.proto.InsertResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.photon.proto.InsertRequest,
      com.photon.proto.InsertResponse> getInsertMethod() {
    io.grpc.MethodDescriptor<com.photon.proto.InsertRequest, com.photon.proto.InsertResponse> getInsertMethod;
    if ((getInsertMethod = IdRegistryServiceGrpc.getInsertMethod) == null) {
      synchronized (IdRegistryServiceGrpc.class) {
        if ((getInsertMethod = IdRegistryServiceGrpc.getInsertMethod) == null) {
          IdRegistryServiceGrpc.getInsertMethod = getInsertMethod =
              io.grpc.MethodDescriptor.<com.photon.proto.InsertRequest, com.photon.proto.InsertResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Insert"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.InsertRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.InsertResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdRegistryServiceMethodDescriptorSupplier("Insert"))
              .build();
        }
      }
    }
    return getInsertMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static IdRegistryServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceStub>() {
        @java.lang.Override
        public IdRegistryServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdRegistryServiceStub(channel, callOptions);
        }
      };
    return IdRegistryServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static IdRegistryServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceBlockingStub>() {
        @java.lang.Override
        public IdRegistryServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdRegistryServiceBlockingStub(channel, callOptions);
        }
      };
    return IdRegistryServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static IdRegistryServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdRegistryServiceFutureStub>() {
        @java.lang.Override
        public IdRegistryServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdRegistryServiceFutureStub(channel, callOptions);
        }
      };
    return IdRegistryServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void lookup(com.photon.proto.LookupRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.LookupResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getLookupMethod(), responseObserver);
    }

    /**
     */
    default void insert(com.photon.proto.InsertRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.InsertResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInsertMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service IdRegistryService.
   */
  public static abstract class IdRegistryServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return IdRegistryServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service IdRegistryService.
   */
  public static final class IdRegistryServiceStub
      extends io.grpc.stub.AbstractAsyncStub<IdRegistryServiceStub> {
    private IdRegistryServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdRegistryServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdRegistryServiceStub(channel, callOptions);
    }

    /**
     */
    public void lookup(com.photon.proto.LookupRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.LookupResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getLookupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void insert(com.photon.proto.InsertRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.InsertResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInsertMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service IdRegistryService.
   */
  public static final class IdRegistryServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<IdRegistryServiceBlockingStub> {
    private IdRegistryServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdRegistryServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdRegistryServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.photon.proto.LookupResponse lookup(com.photon.proto.LookupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getLookupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.photon.proto.InsertResponse insert(com.photon.proto.InsertRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInsertMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service IdRegistryService.
   */
  public static final class IdRegistryServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<IdRegistryServiceFutureStub> {
    private IdRegistryServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdRegistryServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdRegistryServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.photon.proto.LookupResponse> lookup(
        com.photon.proto.LookupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getLookupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.photon.proto.InsertResponse> insert(
        com.photon.proto.InsertRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInsertMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_LOOKUP = 0;
  private static final int METHODID_INSERT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LOOKUP:
          serviceImpl.lookup((com.photon.proto.LookupRequest) request,
              (io.grpc.stub.StreamObserver<com.photon.proto.LookupResponse>) responseObserver);
          break;
        case METHODID_INSERT:
          serviceImpl.insert((com.photon.proto.InsertRequest) request,
              (io.grpc.stub.StreamObserver<com.photon.proto.InsertResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getLookupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.photon.proto.LookupRequest,
              com.photon.proto.LookupResponse>(
                service, METHODID_LOOKUP)))
        .addMethod(
          getInsertMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.photon.proto.InsertRequest,
              com.photon.proto.InsertResponse>(
                service, METHODID_INSERT)))
        .build();
  }

  private static abstract class IdRegistryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    IdRegistryServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.photon.proto.Photon.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("IdRegistryService");
    }
  }

  private static final class IdRegistryServiceFileDescriptorSupplier
      extends IdRegistryServiceBaseDescriptorSupplier {
    IdRegistryServiceFileDescriptorSupplier() {}
  }

  private static final class IdRegistryServiceMethodDescriptorSupplier
      extends IdRegistryServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    IdRegistryServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (IdRegistryServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new IdRegistryServiceFileDescriptorSupplier())
              .addMethod(getLookupMethod())
              .addMethod(getInsertMethod())
              .build();
        }
      }
    }
    return result;
  }
}
