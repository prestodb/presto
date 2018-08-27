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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MockHiveMetastoreClientFactory
        extends HiveMetastoreClientFactory
{
    private Map<HostAndPort, Optional<HiveMetastoreClient>> hostAndPortHiveMetastoreClientMap;

    public MockHiveMetastoreClientFactory(Optional<HostAndPort> socksProxy, Duration timeout, Map<String, Optional<HiveMetastoreClient>> clients)
    {
        super(Optional.empty(), socksProxy, timeout, new NoHiveMetastoreAuthentication());
        this.hostAndPortHiveMetastoreClientMap = clients.entrySet().stream().collect(Collectors.toMap(entry -> createHostAndPort(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public HiveMetastoreClient create(HostAndPort address)
            throws TTransportException
    {
        Optional<HiveMetastoreClient> client = hostAndPortHiveMetastoreClientMap.getOrDefault(address, Optional.empty());
        if (!client.isPresent()) {
            throw new TTransportException(TTransportException.TIMED_OUT);
        }
        return client.get();
    }

    private static HostAndPort createHostAndPort(String str)
    {
        URI uri = URI.create(str);
        return HostAndPort.fromParts(uri.getHost(), uri.getPort());
    }
}
