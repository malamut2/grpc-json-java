package com.github.malamut2;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.reflection.v1alpha.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * {@link GrpcServerRemote} models a remote gRPC server, and accesses the remote server's reflection interface to get
 * full interface syntax for the remote server's services, methods, and message types.
 */
public class GrpcServerRemote {

    private final Channel channel;
    private final ServerReflectionGrpc.ServerReflectionStub serverReflection;

    /**
     * Creates a new instance of {@link GrpcServerRemote}.
     * @param channel the channel to use for communication with the remote gRPC server. The channel is just stored
     *                on object construction, and will only actually be used by the get*() methods.
     */
    public GrpcServerRemote(Channel channel) {
        serverReflection = ServerReflectionGrpc.newStub(channel);
        this.channel = channel;
    }

    /**
     * Convenience constructor which will create a suitable channel for plaintext communication with a
     * remote gRPC server which is identified by its hostname and port.
     * @param hostName the host on which the remote gRPC server resides
     * @param port the port on which the remote gRPC server is listening
     */
    public GrpcServerRemote(String hostName, int port) {
        this(ManagedChannelBuilder.forAddress(hostName, port).usePlaintext().build());
    }

    /**
     * Contacts the remote gRPC server, and obtains a list of the names of its services.
     * @return the names of the services which the remote gRPC server provides.
     * @throws IOException if any problem occurs communicating with the remote gRPC server.
     * @throws InterruptedException if the current thread is interrupted while we wait for replies from the
     * remote gRPC server.
     */
    public List<String> getServiceNames() throws IOException, InterruptedException {
        ListServicesObserver observer = new ListServicesObserver();
        StreamObserver<ServerReflectionRequest> sender = serverReflection.serverReflectionInfo(observer);
        sender.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        sender.onCompleted();
        return observer.getResult();
    }

    /**
     * Contacts the remote gRPC server, and obtains an instance of {@link GrpcServiceRemote} which can subsequently be used
     * to send requests to a selected service of the remote gRPC server.
     * @param name the full name of the gRPC service for which we want to obtain a {@link GrpcServiceRemote} instance.
     * @return a {@link GrpcServiceRemote} instance for the given service.
     * @throws IOException if any problem occurs communicating with the remote gRPC server.
     * @throws InterruptedException if the current thread is interrupted while we wait for replies from the
     * remote gRPC server.
     */
    public GrpcServiceRemote getService(String name) throws IOException, InterruptedException {
        FileDescriptorObserver observer = new FileDescriptorObserver();
        StreamObserver<ServerReflectionRequest> sender = serverReflection.serverReflectionInfo(observer);
        sender.onNext(ServerReflectionRequest.newBuilder().setFileContainingSymbolBytes(ByteString.copyFrom(name, StandardCharsets.UTF_8)).build());
        sender.onCompleted();
        return new GrpcServiceRemote(channel, observer.getResult());
    }

    private static class ListServicesObserver implements StreamObserver<ServerReflectionResponse> {

        private final Object lock = new Object();
        private List<String> result = null;
        private Throwable error = null;
        private final CountDownLatch transmissionOngoing = new CountDownLatch(1);

        @Override
        public void onNext(ServerReflectionResponse serverReflectionResponse) {
            if (serverReflectionResponse.hasListServicesResponse()) {
                ListServiceResponse response = serverReflectionResponse.getListServicesResponse();
                synchronized (lock) {
                    result = response.getServiceList().stream().map(ServiceResponse::getName).toList();
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (lock) {
                error = throwable;
            }
            transmissionOngoing.countDown();
        }

        @Override
        public void onCompleted() {
            transmissionOngoing.countDown();
        }

        public List<String> getResult() throws IOException, InterruptedException {
            transmissionOngoing.await();
            synchronized (lock) {
                if (error != null) {
                    throw new IOException("Could not obtain list of services from remote gRPC server", error);
                }
                if (result == null) {
                    throw new IOException("Remote gRPC server closed connection without transmitting data");
                }
                return result;
            }
        }

    }

    private static class FileDescriptorObserver implements StreamObserver<ServerReflectionResponse> {

        private final Object lock = new Object();
        private List<Descriptors.ServiceDescriptor> result = null;
        private Throwable error = null;
        private final CountDownLatch transmissionOngoing = new CountDownLatch(1);

        @Override
        public void onNext(ServerReflectionResponse serverReflectionResponse) {
            if (serverReflectionResponse.hasFileDescriptorResponse()) {
                List<ByteString> protos = serverReflectionResponse.getFileDescriptorResponse().getFileDescriptorProtoList();
                if (protos.size() != 1) {
                    synchronized (lock) {
                        error = new IOException("We can handle only services based on a single proto file, this service has " + protos.size() + " files.");
                    }
                    return;
                }
                try {
                    DescriptorProtos.FileDescriptorProto fdProto = DescriptorProtos.FileDescriptorProto.parseFrom(protos.get(0));
                    Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdProto, new Descriptors.FileDescriptor[0]);
                    synchronized (lock) {
                        result = fd.getServices();
                    }
                } catch (InvalidProtocolBufferException | Descriptors.DescriptorValidationException e) {
                    synchronized (lock) {
                        error = e;
                    }
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (lock) {
                error = throwable;
            }
            transmissionOngoing.countDown();
        }

        @Override
        public void onCompleted() {
            transmissionOngoing.countDown();
        }

        public List<Descriptors.ServiceDescriptor> getResult() throws IOException, InterruptedException {
            transmissionOngoing.await();
            synchronized (lock) {
                if (error != null) {
                    throw new IOException("Could not obtain service from remote gRPC server", error);
                }
                if (result == null) {
                    throw new IOException("Remote gRPC server closed connection without transmitting data");
                }
                return result;
            }
        }

    }

}
