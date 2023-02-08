package com.github.malamut2;

import com.google.protobuf.*;

import io.grpc.*;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.reflection.v1alpha.*;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DemoMain {

    // protobuf MethodDescriptor -> grpc MethodDescriptor
    // these two static methods are taken mostly from https://stackoverflow.com/a/61144510/725192
    static MethodDescriptor from(
            Descriptors.MethodDescriptor methodDesc
    ) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(getMethodTypeFromDesc(methodDesc))
                .setFullMethodName(MethodDescriptor.generateFullMethodName(
                        methodDesc.getService().getFullName(), methodDesc.getName()))
                .setRequestMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(methodDesc.getInputType())))
                .setResponseMarshaller(ProtoUtils.marshaller(
                        DynamicMessage.getDefaultInstance(methodDesc.getOutputType())))
                .build();
    }

    static MethodDescriptor.MethodType getMethodTypeFromDesc(
            Descriptors.MethodDescriptor methodDesc
    ) {
        if (!methodDesc.isServerStreaming()
                && !methodDesc.isClientStreaming()) {
            return MethodDescriptor.MethodType.UNARY;
        } else if (methodDesc.isServerStreaming()
                && !methodDesc.isClientStreaming()) {
            return MethodDescriptor.MethodType.SERVER_STREAMING;
        } else if (!methodDesc.isServerStreaming()) {
            return MethodDescriptor.MethodType.CLIENT_STREAMING;
        } else {
            return MethodDescriptor.MethodType.BIDI_STREAMING;
        }
    }

    static StreamObserver<ServerReflectionRequest> reflectionInfo;
    static MethodDescriptor grpcFdMethod;
    static DynamicMessage inputParameters;

    public static void main(String[] args) throws InterruptedException {

        if (args.length != 2) {
            System.err.println("Error: we need host and port of the gRPC server as parameters!");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        System.out.println("Contacting " + host + ":" + port);

        CountDownLatch reflectionFinished = new CountDownLatch(1);
        CountDownLatch reflectionDataReceived = new CountDownLatch(1);
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        ServerReflectionGrpc.ServerReflectionStub serverReflection = ServerReflectionGrpc.newStub(channel);
        reflectionInfo = serverReflection.serverReflectionInfo(new StreamObserver<>() {
            @Override
            public void onNext(ServerReflectionResponse serverReflectionResponse) {
                if (serverReflectionResponse.hasListServicesResponse()) {
                    ListServiceResponse response = serverReflectionResponse.getListServicesResponse();
                    List<String> services = response.getServiceList().stream().map(ServiceResponse::getName).filter(name -> !name.startsWith("grpc.")).toList();
                    System.out.println("Services: " + services);
                    for (String service : services) {
                        reflectionInfo.onNext(ServerReflectionRequest.newBuilder().setFileContainingSymbolBytes(ByteString.copyFrom(service, StandardCharsets.UTF_8)).build());
                    }
                }
                if (serverReflectionResponse.hasFileDescriptorResponse()) {
                    List<ByteString> protos = serverReflectionResponse.getFileDescriptorResponse().getFileDescriptorProtoList();
                    System.out.println("Files: " + protos);
                    if (!protos.isEmpty()) {
                        ByteString proto = protos.iterator().next();
                        try {
                            DescriptorProtos.FileDescriptorProto fdProto = DescriptorProtos.FileDescriptorProto.parseFrom(proto);
                            Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdProto, new Descriptors.FileDescriptor[0]);
                            List<Descriptors.ServiceDescriptor> fdServices = fd.getServices();
                            if (!fdServices.isEmpty()) {
                                Descriptors.ServiceDescriptor fdService = fdServices.get(0);
                                List<Descriptors.MethodDescriptor> fdMethods = fdService.getMethods();
                                if (!fdMethods.isEmpty()) {
                                    Descriptors.MethodDescriptor fdMethod = fdMethods.get(0);
                                    synchronized (DemoMain.class) {
                                        grpcFdMethod = from(fdMethod);
                                        Descriptors.Descriptor inputType = fdMethod.getInputType();
                                        inputParameters = DynamicMessage.newBuilder(inputType)
                                                .setField(inputType.findFieldByName("instrumentId"), "ALVG.DE")
                                                .setField(inputType.findFieldByName("maxSamples"), 1)
                                                .build();
                                    }
                                }
                            }
                        } catch (InvalidProtocolBufferException | Descriptors.DescriptorValidationException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    reflectionDataReceived.countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
                reflectionDataReceived.countDown();
                reflectionFinished.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Reflection Done!");
                reflectionDataReceived.countDown();
                reflectionFinished.countDown();
            }
        });
        reflectionInfo.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        reflectionDataReceived.await();
        reflectionInfo.onCompleted();
        reflectionFinished.await();

        synchronized (DemoMain.class) {

            CountDownLatch dataFinished = new CountDownLatch(1);

            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("content-type", Metadata.ASCII_STRING_MARSHALLER), "application/grpc");
            ClientCall clientCall = channel.newCall(grpcFdMethod, CallOptions.DEFAULT);
            clientCall.start(new ClientCall.Listener() {
                @Override
                public void onHeaders(Metadata headers) {
                    System.out.println("Data Call Received metadata: " + headers);
                }

                @Override
                public void onMessage(Object message) {
                    System.out.println("Data Call Received: " + message);
                }

                @Override
                public void onClose(Status status, Metadata trailers) {
                    System.out.println("Data Call Done. " + status + " - " + trailers);
                    dataFinished.countDown();
                }

                @Override
                public void onReady() {
                }
            }, metadata);
            clientCall.sendMessage(inputParameters);
            clientCall.halfClose();
            clientCall.request(1);
            dataFinished.await();

        }

    }

}
