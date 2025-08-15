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

import com.google.common.net.HostAndPort;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class StaticHiveCluster
        implements HiveCluster
{
    private final List<HostAndPort> addresses;
    private final HiveMetastoreClientFactory clientFactory;
    private final String metastoreUsername;
    private final boolean metastoreLoadBalancingEnabled;

    @Inject
    public StaticHiveCluster(StaticMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), config.isMetastoreLoadBalancingEnabled(), config.getMetastoreUsername(), clientFactory);
    }

    public StaticHiveCluster(List<URI> metastoreUris, boolean metastoreLoadBalancingEnabled, String metastoreUsername, HiveMetastoreClientFactory clientFactory)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        this.metastoreLoadBalancingEnabled = metastoreLoadBalancingEnabled;
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.addresses = metastoreUris.stream()
                .map(StaticHiveCluster::checkMetastoreUri)
                .map(uri -> HostAndPort.fromParts(uri.getHost(), uri.getPort()))
                .collect(toList());
        this.metastoreUsername = metastoreUsername;
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
    }

    /**
     * Create a metastore client connected to the Hive metastore.
     * <p>
     * As per Hive HA metastore behavior, return the first metastore in the list
     * list of available metastores (i.e. the default metastore) if a connection
     * can be made, else try another of the metastores at random, until either a
     * connection succeeds or there are no more fallback metastores.
     */
    @Override
    public HiveMetastoreClient createMetastoreClient(Optional<String> token)
            throws TException
    {
        List<HostAndPort> metastores = new ArrayList<>(addresses);
        if (metastoreLoadBalancingEnabled) {
            Collections.shuffle(metastores);
        }

        TException lastException = null;
        for (HostAndPort metastore : metastores) {
            try {
                HiveMetastoreClient client = clientFactory.create(metastore, token);

                if (!isNullOrEmpty(metastoreUsername)) {
                    client.setUGI(metastoreUsername);
                }
                return client;
            }
            catch (TException e) {
                lastException = e;
            }
        }
        throw new TException("Failed connecting to Hive metastore: " + addresses, lastException);
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }
}
