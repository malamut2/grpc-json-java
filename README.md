# grpc-json-java

## Purpose

This is a small library which can be used to access remote gRPC services
from java code even if neither the service specific client nor its
proto file is available. For parameter input and message output,
we use json, similar to grpcurl. This should work on all gRPC servers which
expose the gRPC standard reflection API.

Also, this code can be used as a starting point for your own development
to cover any other needs related to access gRPC services without 
the service-specific client. It is MIT-licensed, so feel free to go ahead
and derive from it according to your needs.

## How to Build

Prerequisite to building the library are 
[Java 17 (or higher)](https://adoptium.net/) and
[Maven 3.8 (or higher)](https://maven.apache.org/download.cgi). 
From the directory in which this `README` file resides, simply issue

`mvn package`

at the command line. The full library, excluding dependencies and test classes,
will be written to

`target/grpc-json-java-<version>.jar`

## How to use

Have a look at the demo class `DemoMain` and at the classes' javadoc code to
learn how to use the code. `DemoMain` contains a main method, so it can be
called as such:

`java DemoMain localhost 6969 myService myMethod {\"myParameter\":123}`

This will contact a gRPC server running on your local machine on port `6969`,
and execute `myService.myMethod`, using default input parameters except for
`myParameter`, which will be set to `123`.