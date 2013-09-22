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
package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.net.HostAndPort;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StaticHiveCluster
        implements HiveCluster
{
    private final HostAndPort address;
    private final HiveMetastoreClientFactory clientFactory;

    public StaticHiveCluster(HostAndPort address, HiveMetastoreClientFactory clientFactory)
    {
        checkNotNull(address, "address is null");
        checkArgument(address.hasPort(), "address does not have a port");
        checkNotNull(clientFactory, "clientFactory is null");

        this.address = address;
        this.clientFactory = clientFactory;
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        try {
            return clientFactory.create(address.getHostText(), address.getPort());
        }
        catch (TTransportException e) {
            throw new RuntimeException("Failed connecting to Hive metastore: " + address, e);
        }
    }
}
