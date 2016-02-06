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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;

import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CassandraClientModule
        implements Module
{
    private final String connectorId;

    public CassandraClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(CassandraConnectorId.class).toInstance(new CassandraConnectorId(connectorId));
        binder.bind(CassandraConnector.class).in(Scopes.SINGLETON);
        binder.bind(CassandraMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTokenSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraConnectorRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPartitionManager.class).in(Scopes.SINGLETON);

        binder.bind(CassandraThriftConnectionFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(CassandraClientConfig.class);

        binder.bind(CassandraThriftConnectionFactory.class).in(Scopes.SINGLETON);

        binder.bind(CachingCassandraSchemaProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingCassandraSchemaProvider.class).as(generatedNameOf(CachingCassandraSchemaProvider.class, connectorId));

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
    }

    @ForCassandra
    @Singleton
    @Provides
    public static ExecutorService createCachingCassandraSchemaExecutor(CassandraConnectorId clientId, CassandraClientConfig cassandraClientConfig)
    {
        return newFixedThreadPool(
                cassandraClientConfig.getMaxSchemaRefreshThreads(),
                daemonThreadsNamed("cassandra-" + clientId + "-%s"));
    }

    @Singleton
    @Provides
    public static CassandraSession createCassandraSession(
            CassandraConnectorId connectorId,
            CassandraClientConfig config,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        Cluster.Builder clusterBuilder = Cluster.builder();

        List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
        checkArgument(!contactPoints.isEmpty(), "empty contactPoints");
        clusterBuilder.addContactPoints(contactPoints.toArray(new String[contactPoints.size()]));

        clusterBuilder.withPort(config.getNativeProtocolPort());
        clusterBuilder.withReconnectionPolicy(new ExponentialReconnectionPolicy(500, 10000));
        clusterBuilder.withRetryPolicy(config.getRetryPolicy().getPolicy());

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(Ints.checkedCast(config.getClientReadTimeout().toMillis()));
        socketOptions.setConnectTimeoutMillis(Ints.checkedCast(config.getClientConnectTimeout().toMillis()));
        if (config.getClientSoLinger() != null) {
            socketOptions.setSoLinger(config.getClientSoLinger());
        }
        clusterBuilder.withSocketOptions(socketOptions);

        if (config.getUsername() != null && config.getPassword() != null) {
            clusterBuilder.withCredentials(config.getUsername(), config.getPassword());
        }

        QueryOptions options = new QueryOptions();
        options.setFetchSize(config.getFetchSize());
        options.setConsistencyLevel(config.getConsistencyLevel());
        clusterBuilder.withQueryOptions(options);

        return new CassandraSession(
                connectorId.toString(),
                clusterBuilder,
                config.getFetchSizeForPartitionKeySelect(),
                config.getLimitForPartitionKeySelect(),
                extraColumnMetadataCodec);
    }
}
