/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.udf.grpc;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.BlockEncodingManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class TestingGrpcUdfServer
{
    Logger log = Logger.get(EchoFirstInputGrpcUdfService.class);
    private static int port = 50051;
    private static Server server;

    private TestingGrpcUdfServer()
    {
        server = ServerBuilder.forPort(port)
                .addService(new EchoFirstInputGrpcUdfService(new BlockEncodingManager()))
                .build();
    }

    private void startGrpcServer()
            throws IOException
    {
        server.start();
        log.info("======== SERVER STARTED ========");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run()
            {
                log.info("======== shutting down gRPC server since JVM is shutting down ========");
                stopGrpcServer();
                log.info("======== server shut down ========");
            }
        });
    }

    private void stopGrpcServer()
    {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown()
            throws InterruptedException
    {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        TestingGrpcUdfServer testingGrpcUdfServer = new TestingGrpcUdfServer();
        testingGrpcUdfServer.startGrpcServer();
        testingGrpcUdfServer.blockUntilShutdown();
    }
}
