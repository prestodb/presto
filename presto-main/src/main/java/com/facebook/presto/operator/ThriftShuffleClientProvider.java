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
package com.facebook.presto.operator;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.server.thrift.ThriftTaskClient;
import com.google.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class ThriftShuffleClientProvider
        implements RpcShuffleClientProvider
{
    private final DriftClient<ThriftTaskClient> httpClient;

    @Inject
    public ThriftShuffleClientProvider(@ForExchange DriftClient<ThriftTaskClient> httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public RpcShuffleClient get(URI location)
    {
        return new ThriftRpcShuffleClient(httpClient, location);
    }
}
