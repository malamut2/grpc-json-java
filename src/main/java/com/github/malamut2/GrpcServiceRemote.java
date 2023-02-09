package com.github.malamut2;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.*;
import io.grpc.protobuf.ProtoUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class GrpcServiceRemote {

    private final Channel channel;
    private final List<Descriptors.ServiceDescriptor> serviceDescriptors;

    protected GrpcServiceRemote(Channel channel, List<Descriptors.ServiceDescriptor> serviceDescriptors) {
        this.channel = channel;
        this.serviceDescriptors = serviceDescriptors;
    }

    public Descriptors.ServiceDescriptor findService(String serviceName) {
        return find(serviceDescriptors, Descriptors.ServiceDescriptor::getFullName, serviceName);
    }

    public Descriptors.MethodDescriptor findMethod(String serviceName, String methodName) {
        Descriptors.ServiceDescriptor service = findService(serviceName);
        if (service == null) {
            return null;
        }
        List<Descriptors.MethodDescriptor> methods = service.getMethods();
        return find(methods, Descriptors.MethodDescriptor::getFullName, methodName);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Message request(Descriptors.MethodDescriptor method, String json) throws InterruptedException, IOException {

        Descriptors.Descriptor inputType = method.getInputType();
        DynamicMessage.Builder inputParamBuilder = DynamicMessage.newBuilder(inputType);
        JsonFormat.parser().merge(json, inputParamBuilder);
        DynamicMessage inputParameters = inputParamBuilder.build();

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("content-type", Metadata.ASCII_STRING_MARSHALLER), "application/grpc");
        ClientCall clientCall = channel.newCall(toGrpc(method), CallOptions.DEFAULT);
        ClientCallListener<Message> listener = new ClientCallListener<>();
        clientCall.start(listener, metadata);
        clientCall.sendMessage(inputParameters);
        clientCall.halfClose();
        clientCall.request(1);
        return listener.getResult();

    }

    private static <T> T find(List<T> list, Function<T, String> textMapper, String text) {
        T result = list.stream().filter(t -> Objects.equals(text, textMapper.apply(t))).findFirst().orElse(null);
        if (result == null) {
            String suffix = "." + text;
            result = list.stream().filter(t -> {
                String instance = textMapper.apply(t);
                return instance != null && instance.endsWith(suffix);
            }).findFirst().orElse(null);
        }
        return result;
    }

    // protobuf MethodDescriptor -> grpc MethodDescriptor
    // --- begin methods taken mostly from https://stackoverflow.com/a/61144510/725192
    @SuppressWarnings("rawtypes")
    private static MethodDescriptor toGrpc(Descriptors.MethodDescriptor methodDesc) {
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

    private static MethodDescriptor.MethodType getMethodTypeFromDesc(Descriptors.MethodDescriptor methodDesc) {
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
    // --- end methods taken mostly from https://stackoverflow.com/a/61144510/725192

    private static class ClientCallListener<T> extends ClientCall.Listener<T> {

        private final Object lock = new Object();
        private final CountDownLatch transmissionOngoing = new CountDownLatch(1);
        private T result;
        private StatusRuntimeException error = null;

        @Override
        public void onHeaders(Metadata headers) {
        }

        @Override
        public void onMessage(T message) {
            synchronized (lock) {
                result = message;
            }
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            if (!status.isOk()) {
                synchronized (lock) {
                    error = new StatusRuntimeException(status, trailers);
                }
            }
            transmissionOngoing.countDown();
        }

        @Override
        public void onReady() {
        }

        public T getResult() throws InterruptedException, IOException {
            transmissionOngoing.await();
            synchronized (lock) {
                if (error != null) {
                    throw new IOException("Could not obtain result from remote gRPC server", error);
                }
                if (result == null) {
                    throw new IOException("Remote gRPC server closed connection without transmitting data");
                }
                return result;
            }

        }

    }

}
