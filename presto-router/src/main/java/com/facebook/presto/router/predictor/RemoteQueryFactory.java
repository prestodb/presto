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
package com.facebook.presto.router.predictor;

import com.facebook.airlift.http.client.HttpClient;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class RemoteQueryFactory
{
    private static final String QUERY_CPU_URI = "/cpu";
    private static final String QUERY_MEMORY_URI = "/memory";
    private final HttpClient queryCpuHttpClient;
    private final HttpClient queryMemoryHttpClient;

    @Inject
    public RemoteQueryFactory(
            @ForQueryCpuPredictor HttpClient queryCpuHttpClient,
            @ForQueryMemoryPredictor HttpClient queryMemoryHttpClient)
    {
        this.queryCpuHttpClient = requireNonNull(queryCpuHttpClient, "Http client for CPU prediction is null");
        this.queryMemoryHttpClient = requireNonNull(queryMemoryHttpClient, "Http client for memory prediction is null");
    }

    public RemoteQueryCpu createRemoteQueryCPU(URI uri)
    {
        return new RemoteQueryCpu(queryCpuHttpClient, uriBuilderFrom(uri).appendPath(QUERY_CPU_URI).build());
    }

    public RemoteQueryMemory createRemoteQueryMemory(URI uri)
    {
        return new RemoteQueryMemory(queryMemoryHttpClient, uriBuilderFrom(uri).appendPath(QUERY_MEMORY_URI).build());
    }
}
