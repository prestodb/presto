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

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class StaticHiveCluster
        implements HiveCluster
{
    private final HostAndPort address;
    private final HiveMetastoreClientFactory clientFactory;

    @Inject
    public StaticHiveCluster(StaticMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        URI uri = checkMetastoreUri(config.getMetastoreUri());
        this.address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        this.clientFactory = checkNotNull(clientFactory, "clientFactory is null");
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

    private static URI checkMetastoreUri(URI uri)
    {
        checkNotNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }
}
