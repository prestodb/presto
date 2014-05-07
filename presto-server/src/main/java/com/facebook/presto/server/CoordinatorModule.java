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
package com.facebook.presto.server;

import com.facebook.presto.execution.DropTableExecution;
import com.facebook.presto.execution.NodeScheduler;
import com.facebook.presto.execution.NodeSchedulerConfig;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.SqlQueryExecution;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.facebook.presto.metadata.DatabaseShardManager;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.ForShardCleaner;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.NativeConnectorId;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NativeRecordSinkProvider;
import com.facebook.presto.metadata.ShardCleaner;
import com.facebook.presto.metadata.ShardCleanerConfig;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativeSplitManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.UseCollection;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // TODO: currently, this module is ALWAYS installed (even for non-coordinators)

        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

        discoveryBinder(binder).bindSelector("presto");

        // query manager
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(StageResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        bindConfig(binder).to(QueryManagerConfig.class);

        // analyzer
        bindConfig(binder).to(FeaturesConfig.class);

        // native
        binder.bind(NativeConnectorId.class).toInstance(new NativeConnectorId("default"));
        binder.bind(NativeMetadata.class).in(Scopes.SINGLETON);
        binder.bind(NativeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(NativeDataStreamProvider.class).in(Scopes.SINGLETON);
        binder.bind(NativeRecordSinkProvider.class).in(Scopes.SINGLETON);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // node scheduler
        binder.bind(InternalNodeManager.class).to(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeManager.class).to(Key.get(InternalNodeManager.class)).in(Scopes.SINGLETON);
        bindConfig(binder).to(NodeSchedulerConfig.class);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeScheduler.class).withGeneratedName();

        // shard management
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(ShardCleanerConfig.class);
        binder.bind(ShardCleaner.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("shard-cleaner", ForShardCleaner.class);
        binder.bind(ShardResource.class).in(Scopes.SINGLETON);

        // query execution
        binder.bind(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SqlQueryExecution.SqlQueryExecutionFactory.class).withGeneratedName();

        MapBinder<Class<? extends Statement>, QueryExecution.QueryExecutionFactory<?>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {},
                new TypeLiteral<QueryExecution.QueryExecutionFactory<?>>() {}
        );

        binder.bind(DropTableExecution.DropTableExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(DropTable.class).to(DropTableExecution.DropTableExecutionFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DropTableExecution.DropTableExecutionFactory.class).withGeneratedName();

        executionBinder.addBinding(Query.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Explain.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowColumns.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowPartitions.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowFunctions.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowTables.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowSchemas.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowCatalogs.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(UseCollection.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(CreateTable.class).to(SqlQueryExecution.SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
    }
}
