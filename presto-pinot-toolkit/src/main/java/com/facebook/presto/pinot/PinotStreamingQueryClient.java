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
package com.facebook.presto.pinot;

import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.grpc.ServerGrpcQueryClient;
import org.apache.pinot.common.utils.grpc.ServerGrpcRequestBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Grpc based Pinot query client.
 * This is part of the presto-pinot driver from the official pinot repo.
 * Support has been dropped in recent versions, so we've moved it here.
 * <a href="https://github.com/apache/pinot/blob/6e235a4ec2a16006337da04e118a435b5bb8f6d8/pinot-connectors/prestodb-pinot-dependencies/presto-pinot-driver/src/main/java/org/apache/pinot/connector/presto/grpc/PinotStreamingQueryClient.java">Original reference</a>
 */
public class PinotStreamingQueryClient
{
    private final Map<String, ServerGrpcQueryClient> grpcQueryClientMap = new HashMap<>();
    private final GrpcConfig config;

    public PinotStreamingQueryClient(GrpcConfig config)
    {
        this.config = config;
    }

    public Iterator<Server.ServerResponse> submit(String host, int port, ServerGrpcRequestBuilder requestBuilder)
    {
        ServerGrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
        return client.submit(requestBuilder.build());
    }

    private ServerGrpcQueryClient getOrCreateGrpcQueryClient(String host, int port)
    {
        String key = String.format("%s_%d", host, port);
        if (!grpcQueryClientMap.containsKey(key)) {
            grpcQueryClientMap.put(key, new ServerGrpcQueryClient(host, port, config));
        }
        return grpcQueryClientMap.get(key);
    }
}
