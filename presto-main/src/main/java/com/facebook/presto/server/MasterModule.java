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

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.AddColumnTask;
import com.facebook.presto.execution.CallTask;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateSchemaTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DispatchQueryExecution.DispatchQueryExecutionFactory;
import com.facebook.presto.execution.DropColumnTask;
import com.facebook.presto.execution.DropSchemaTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.GrantTask;
import com.facebook.presto.execution.LazyOutput;
import com.facebook.presto.execution.LazyProxyQueryManager;
import com.facebook.presto.execution.LazyQueryExecution.LazyQueryExecutionFactory;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecutionMBean;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryOutputInfo;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameSchemaTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RevokeTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.UseTask;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.AllAtOnceExecutionPolicy;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.PhasedExecutionPolicy;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.ForMemoryManager;
import com.facebook.presto.memory.LowMemoryKiller;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryManagerConfig.LowMemoryKillerPolicy;
import com.facebook.presto.memory.NoneLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.protocol.DispatchQuery.DispatchQueryFactory;
import com.facebook.presto.server.protocol.LazyQuery.LazyQueryFactory;
import com.facebook.presto.server.protocol.Query.QueryFactory;
import com.facebook.presto.server.protocol.SqlQuery.SqlQueryFactory;
import com.facebook.presto.server.protocol.StatementResource;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import static com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.facebook.presto.util.StatementUtils.getAllQueryTypes;
import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MasterModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

        if (serverConfig.isDispatcher()) {
            // presto dispatcher announcement
            discoveryBinder(binder).bindHttpAnnouncement("presto-dispatcher");
        }
        if (serverConfig.isCoordinator()) {
            // presto coordinator announcement
            discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");
        }

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jaxrsBinder(binder).bind(StatementResource.class);

        // query execution visualizer
        jaxrsBinder(binder).bind(QueryExecutionResource.class);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);

        if (serverConfig.isDispatcher()) {
            binder.bind(DispatchQueryFactory.class).in(Scopes.SINGLETON);
        }
        if (serverConfig.isCoordinator()) {
            binder.bind(SqlQueryFactory.class).in(Scopes.SINGLETON);
        }

        if (serverConfig.isDispatcher() && serverConfig.isCoordinator()) {
            binder.bind(new TypeLiteral<QueryManager<LazyOutput>>() {}).to(new TypeLiteral<SqlQueryManager<LazyOutput>>() {}).in(Scopes.SINGLETON);
            // create proxy query managers for dispatch and sql query factory
            binder.bind(new TypeLiteral<QueryManager<QueryResults>>() {}).to(new TypeLiteral<LazyProxyQueryManager<QueryResults>>() {}).in(Scopes.SINGLETON);
            binder.bind(new TypeLiteral<QueryManager<QueryOutputInfo>>() {}).to(new TypeLiteral<LazyProxyQueryManager<QueryOutputInfo>>() {}).in(Scopes.SINGLETON);
            binder.bind(QueryManager.class).to(new TypeLiteral<QueryManager<LazyOutput>>() {});
            binder.bind(QueryFactory.class).to(LazyQueryFactory.class).in(Scopes.SINGLETON);
        }
        else if (serverConfig.isDispatcher() && !serverConfig.isCoordinator()) {
            binder.bind(new TypeLiteral<QueryManager<QueryResults>>() {}).to(new TypeLiteral<SqlQueryManager<QueryResults>>() {}).in(Scopes.SINGLETON);
            binder.bind(QueryManager.class).to(new TypeLiteral<QueryManager<QueryResults>>() {});
            binder.bind(QueryFactory.class).to(DispatchQueryFactory.class).in(Scopes.SINGLETON);
        }
        else {
            verify(!serverConfig.isDispatcher() && serverConfig.isCoordinator());
            binder.bind(new TypeLiteral<QueryManager<QueryOutputInfo>>() {}).to(new TypeLiteral<SqlQueryManager<QueryOutputInfo>>() {}).in(Scopes.SINGLETON);
            binder.bind(QueryManager.class).to(new TypeLiteral<QueryManager<QueryOutputInfo>>() {});
            binder.bind(QueryFactory.class).to(SqlQueryFactory.class).in(Scopes.SINGLETON);
        }
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();

        // cluster memory manager
        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        bindLowMemoryKiller(LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // cluster statistics
        jaxrsBinder(binder).bind(ClusterStatsResource.class);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // execution scheduler
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                });

        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        // query execution
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class).as(generatedNameOf(QueryExecution.class));

        if (serverConfig.isDispatcher()) {
            binder.bind(ScheduledExecutorService.class).annotatedWith(ForQueryExecution.class)
                    .toInstance(newSingleThreadScheduledExecutor(threadsNamed("dispatch-scheduler")));

            httpClientBinder(binder).bindHttpClient("queryExecutionFactory", ForQueryExecution.class)
                    .withTracing()
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(10, SECONDS));
                        config.setRequestTimeout(new Duration(20, SECONDS));
                    });
            binder.bind(DispatchQueryExecutionFactory.class).in(Scopes.SINGLETON);
        }

        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();
        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>>>() {});

        getAllQueryTypes().entrySet().stream()
                .filter(entry -> entry.getValue() != QueryType.DATA_DEFINITION)
                .forEach(entry -> executionBinder.addBinding(entry.getKey()).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON));

        binder.bind(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        bindDataDefinitionTask(binder, executionBinder, CreateSchema.class, CreateSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropSchema.class, DropSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameSchema.class, RenameSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, AddColumn.class, AddColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateTable.class, CreateTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameTable.class, RenameTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameColumn.class, RenameColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropColumn.class, DropColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropTable.class, DropTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateView.class, CreateViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropView.class, DropViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, Use.class, UseTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(binder, executionBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(binder, executionBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(binder, executionBinder, Grant.class, GrantTask.class);
        bindDataDefinitionTask(binder, executionBinder, Revoke.class, RevokeTask.class);
        bindDataDefinitionTask(binder, executionBinder, Prepare.class, PrepareTask.class);
        bindDataDefinitionTask(binder, executionBinder, Deallocate.class, DeallocateTask.class);

        if (serverConfig.isDispatcher() && serverConfig.isCoordinator()) {
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<LazyOutput>>> executionFactoryBinder = newMapBinder(binder,
                    new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<? extends QueryExecution<LazyOutput>>>() {});

            getAllQueryTypes().keySet().forEach(statement -> executionFactoryBinder.addBinding(statement).to(LazyQueryExecutionFactory.class).in(Scopes.SINGLETON));
        }
        else if (serverConfig.isDispatcher() && !serverConfig.isCoordinator()) {
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<QueryResults>>> executionFactoryBinder = newMapBinder(binder,
                    new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<? extends QueryExecution<QueryResults>>>() {});

            getAllQueryTypes().keySet().forEach(statement -> executionFactoryBinder.addBinding(statement).to(DispatchQueryExecutionFactory.class).in(Scopes.SINGLETON));
        }
        else {
            verify(!serverConfig.isDispatcher() && serverConfig.isCoordinator());
        }

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<? extends QueryExecution<QueryOutputInfo>>> executionBinder,
            Class<T> statement,
            Class<? extends DataDefinitionTask<T>> task)
    {
        verify(getAllQueryTypes().get(statement) == QueryType.DATA_DEFINITION);
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

        taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
        executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
    }

    private void bindLowMemoryKiller(String name, Class<? extends LowMemoryKiller> clazz)
    {
        install(installModuleIf(
                MemoryManagerConfig.class,
                config -> name.equals(config.getLowMemoryKillerPolicy()),
                binder -> binder.bind(LowMemoryKiller.class).to(clazz).in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
