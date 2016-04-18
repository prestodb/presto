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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.AddColumnTask;
import com.facebook.presto.execution.CallTask;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.GrantTask;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecutionMBean;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryQueueManager;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.SqlQueryQueueManager;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.scheduler.AllAtOnceExecutionPolicy;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerExporter;
import com.facebook.presto.execution.scheduler.PhasedExecutionPolicy;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.ForGracefulShutdown;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Use;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.units.Duration;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import static com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // TODO: currently, this module is ALWAYS installed (even for non-coordinators)
        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

        discoveryBinder(binder).bindSelector("presto");

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(QueryQueueManager.class).to(SqlQueryQueueManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        configBinder(binder).bindConfig(QueryManagerConfig.class);

        // analyzer
        configBinder(binder).bindConfig(FeaturesConfig.class);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // session properties
        binder.bind(SessionPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);

        // table properties
        binder.bind(TablePropertyManager.class).in(Scopes.SINGLETON);

        // node partitioning manager
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);

        // node scheduler
        binder.bind(InternalNodeManager.class).to(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(NodeManager.class).to(Key.get(InternalNodeManager.class)).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        binder.bind(NodeSchedulerExporter.class).in(Scopes.SINGLETON);
        binder.bind(NodeTaskMap.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeScheduler.class).withGeneratedName();

        // for polling worker states for graceful shutdown
        httpClientBinder(binder).bindHttpClient("node-manager", ForGracefulShutdown.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });

        // query execution
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class).as(generatedNameOf(QueryExecution.class));

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<?>>() {});

        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Query.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Explain.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowColumns.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowPartitions.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowFunctions.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowTables.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowSchemas.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowCatalogs.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Use.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(ShowSession.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(CreateTableAsSelect.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Insert.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        executionBinder.addBinding(Delete.class).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);

        binder.bind(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        bindDataDefinitionTask(binder, executionBinder, AddColumn.class, AddColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateTable.class, CreateTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameTable.class, RenameTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameColumn.class, RenameColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropTable.class, DropTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateView.class, CreateViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropView.class, DropViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(binder, executionBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(binder, executionBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(binder, executionBinder, Grant.class, GrantTask.class);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        jsonCodecBinder(binder).bindJsonCodec(ViewDefinition.class);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder,
            Class<T> statement,
            Class<? extends DataDefinitionTask<T>> task)
    {
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

        taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
        executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
    }
}
