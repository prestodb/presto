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
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Grpc based Pinot query client.
 */
public class PinotStreamingQueryClient
{
    private final Map<String, GrpcQueryClient> grpcQueryClientMap = new HashMap<>();
    private final GrpcConfig config;

    public PinotStreamingQueryClient(GrpcConfig config)
    {
        this.config = config;
    }

    public Iterator<Server.ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder)
    {
        GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
        return client.submit(requestBuilder.build());
    }

    private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port)
    {
        String key = String.format("%s_%d", host, port);
        if (!grpcQueryClientMap.containsKey(key)) {
            grpcQueryClientMap.put(key, new GrpcQueryClient(host, port, config));
        }
        return grpcQueryClientMap.get(key);
    }
}
