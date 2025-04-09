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
package com.facebook.presto.hive.metastore.hms;

import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StaticHiveCluster
        implements HiveCluster
{
    private final ImmutableList<URI> metastoreUris;
    private final MetastoreClientFactory clientFactory;
    private final String metastoreUsername;
    private final boolean metastoreLoadBalancingEnabled;

    @Inject
    public StaticHiveCluster(StaticMetastoreConfig config, MetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), config.isMetastoreLoadBalancingEnabled(), config.getMetastoreUsername(), clientFactory);
    }

    public StaticHiveCluster(List<URI> metastoreUris, boolean metastoreLoadBalancingEnabled, String metastoreUsername, MetastoreClientFactory clientFactory)
    {
        requireNonNull(metastoreUris, "metastoreUris is null");
        this.metastoreLoadBalancingEnabled = metastoreLoadBalancingEnabled;
        checkArgument(!metastoreUris.isEmpty(), "metastoreUris must specify at least one URI");
        this.metastoreUris = metastoreUris.stream()
                .map(StaticHiveCluster::checkMetastoreUri)
                .collect(toImmutableList());
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
        if (metastoreLoadBalancingEnabled) {
            Collections.shuffle(metastoreUris);
        }

        TException lastException = null;
        for (URI metastoreUri : metastoreUris) {
            try {
                HiveMetastoreClient client = clientFactory.create(metastoreUri, token);

                if (!isNullOrEmpty(metastoreUsername)) {
                    client.setUGI(metastoreUsername);
                }
                return client;
            }
            catch (TException e) {
                lastException = e;
            }
        }
        throw new TException("Failed connecting to Hive metastore: " + metastoreUris, lastException);
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }
}
