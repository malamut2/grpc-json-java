package com.github.malamut2;

import com.google.protobuf.Descriptors;

import java.io.IOException;
import java.util.List;

public class DemoMain {

    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length != 5) {
            System.err.println("""
            Error: we need the following parameters:
            1. host of the gRPC server
            2. port of the gRPC server
            3. name of the service (fully qualified, or just the last name)
            4. name of the method (fully qualified, or just the last name)
            5. input parameters as json
            """);
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String serviceName = args[2];
        String methodName = args[3];
        String jsonInputParameters = args[4];
        System.out.println(
                "Contacting " + host + ":" + port
                        + ", executing in service " + serviceName + " method " + methodName
                        + " with input parameters " + jsonInputParameters
        );

        GrpcServerRemote server = new GrpcServerRemote(host, port);
        List<String> services = server.getServiceNames();
        System.out.println("Services: " + services);

        String fullServiceName = services.stream().filter(name -> !name.startsWith("grpc.")).findFirst().orElse(null);
        GrpcServiceRemote service = server.getService(fullServiceName);

        Descriptors.MethodDescriptor method = service.findMethod(serviceName, methodName);
        Object result = service.request(method, jsonInputParameters);
        System.out.println("Result:\n" + result);

    }

}
