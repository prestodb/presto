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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.configuration.ConfigurationModule.bindConfig;
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
        binder.bind(CassandraMetadata.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraHandleResolver.class).in(Scopes.SINGLETON);

        bindConfig(binder).to(CassandraClientConfig.class);

        binder.bind(CachingCassandraSchemaProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingCassandraSchemaProvider.class).as(generatedNameOf(CachingCassandraSchemaProvider.class, connectorId));

        binder.bind(CassandraSessionFactory.class).in(Scopes.SINGLETON);
    }

    @ForCassandraSchema
    @Singleton
    @Provides
    public ExecutorService createCachingCassandraSchemaExecutor(CassandraConnectorId clientId, CassandraClientConfig cassandraClientConfig)
    {
        return Executors.newFixedThreadPool(
                cassandraClientConfig.getMaxSchemaRefreshThreads(),
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat("cassandra-schema-" + clientId + "-%d").build());
    }

    @Singleton
    @Provides
    public CassandraSession createCassandraSession(CassandraConnectorId connectorId, CassandraClientConfig config)
    {
        CassandraSessionFactory factory = new CassandraSessionFactory(connectorId, config);
        return factory.create();
    }
}
