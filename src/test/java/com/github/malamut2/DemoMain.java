package com.github.malamut2;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.util.List;

/**
 * {@link DemoMain} demonstrates the use of the grpc-json-java library. Prerequisite is a running gRPC server which
 * supports reflection. DemoMain will connect to this server, and execute a method of your choice.
 */
public class DemoMain {

    public static void main(String[] args) throws InterruptedException, IOException {

        // --- Some boilerplate code to make this short tool easy to use

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

        // --- Create an instance of GrpcRemoteServer first. The server will not be contacted yet.

        GrpcServerRemote server = new GrpcServerRemote(host, port);

        // --- Connect to the server, and use reflection to get a list of all services hosted by this server.

        List<String> services = server.getServiceNames();

        // --- Print a list of those services to the screen.

        System.out.println("Services: " + services);

        // --- Filter away all infrastructure services. In most cases, that leaves only one service, which we will use.

        String fullServiceName = services.stream().filter(name -> !name.startsWith("grpc.")).findFirst().orElse(null);

        // --- Connect to the server, and use reflection to get full information on a single selected service.
        //     This method can be called independently. If you know the fully-qualified service name,
        //     there is no need to call getServiceNames() first.

        GrpcServiceRemote service = server.getService(fullServiceName);

        // --- For the downloaded service information, get the method we want to call

        Descriptors.MethodDescriptor method = service.findMethod(serviceName, methodName);

        // --- Perform a remote call to that method, using our parameters in JSON format

        Message result = service.request(method, jsonInputParameters);

        // --- The result object will be a fully-valid gRPC message. We output it in JSON format.

        System.out.println("Result:\n" + JsonFormat.printer().print(result));

    }

}
