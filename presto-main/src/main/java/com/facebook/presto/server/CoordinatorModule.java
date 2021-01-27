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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.discovery.server.EmbeddedDiscoveryModule;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculator.EstimatedExchanges;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.StatsCalculatorModule;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.dispatcher.DispatchExecutor;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.dispatcher.DispatchQueryFactory;
import com.facebook.presto.dispatcher.FailedDispatchQueryFactory;
import com.facebook.presto.dispatcher.LocalDispatchQueryFactory;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.execution.AddColumnTask;
import com.facebook.presto.execution.AlterFunctionTask;
import com.facebook.presto.execution.CallTask;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateFunctionTask;
import com.facebook.presto.execution.CreateRoleTask;
import com.facebook.presto.execution.CreateSchemaTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DropColumnTask;
import com.facebook.presto.execution.DropFunctionTask;
import com.facebook.presto.execution.DropRoleTask;
import com.facebook.presto.execution.DropSchemaTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.GrantRolesTask;
import com.facebook.presto.execution.GrantTask;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecutionMBean;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameSchemaTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RevokeRolesTask;
import com.facebook.presto.execution.RevokeTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetRoleTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.UseTask;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.AllAtOnceExecutionPolicy;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.PhasedExecutionPolicy;
import com.facebook.presto.execution.scheduler.SectionExecutionFactory;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.ForMemoryManager;
import com.facebook.presto.memory.LowMemoryKiller;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryManagerConfig.LowMemoryKillerPolicy;
import com.facebook.presto.memory.NoneLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.server.protocol.ExecutingStatementResource;
import com.facebook.presto.server.protocol.LocalQueryProvider;
import com.facebook.presto.server.protocol.QueuedStatementResource;
import com.facebook.presto.server.remotetask.HttpRemoteTaskFactory;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AlterFunction;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateRole;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropFunction;
import com.facebook.presto.sql.tree.DropRole;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.GrantRoles;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetRole;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.transaction.ForTransactionManager;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.server.HttpServerBinder.httpServerBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import static com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.facebook.presto.util.StatementUtils.getAllQueryTypes;
import static com.google.common.base.Verify.verify;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");
        httpServerBinder(binder).bindResource("/tableau", "webapp/tableau");

        // discovery server
        install(installModuleIf(EmbeddedDiscoveryConfig.class, EmbeddedDiscoveryConfig::isEnabled, new EmbeddedDiscoveryModule()));

        // presto coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jsonCodecBinder(binder).bindJsonCodec(SelectedRole.class);
        jaxrsBinder(binder).bind(QueuedStatementResource.class);
        jaxrsBinder(binder).bind(ExecutingStatementResource.class);
        binder.bind(StatementHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StatementHttpExecutionMBean.class).withGeneratedName();

        // resource for serving static content
        jaxrsBinder(binder).bind(WebUiResource.class);

        // failure detector
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);

        // query monitor
        jsonCodecBinder(binder).bindJsonCodec(OperatorInfo.class);
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        binder.bind(LocalQueryProvider.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(TaskInfoResource.class);

        // dispatcher
        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        // local dispatcher
        binder.bind(DispatchQueryFactory.class).to(LocalDispatchQueryFactory.class);

        // cluster memory manager
        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        bindLowMemoryKiller(LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // node monitor
        binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);

        // statistics calculator
        binder.install(new StatsCalculatorModule());

        // cost calculator
        binder.bind(TaskCountEstimator.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // cluster statistics
        jaxrsBinder(binder).bind(ClusterStatsResource.class);

        // planner
        binder.bind(PlanFragmenter.class).in(Scopes.SINGLETON);
        binder.bind(PlanOptimizers.class).in(Scopes.SINGLETON);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // explain analyze
        binder.bind(ExplainAnalyzeContext.class).in(Scopes.SINGLETON);

        // execution scheduler
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
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

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<?>>() {});

        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();
        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(SectionExecutionFactory.class).in(Scopes.SINGLETON);
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
        bindDataDefinitionTask(binder, executionBinder, CreateFunction.class, CreateFunctionTask.class);
        bindDataDefinitionTask(binder, executionBinder, AlterFunction.class, AlterFunctionTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropFunction.class, DropFunctionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Use.class, UseTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(binder, executionBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(binder, executionBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateRole.class, CreateRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropRole.class, DropRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, GrantRoles.class, GrantRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, RevokeRoles.class, RevokeRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetRole.class, SetRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, Grant.class, GrantTask.class);
        bindDataDefinitionTask(binder, executionBinder, Revoke.class, RevokeTask.class);
        bindDataDefinitionTask(binder, executionBinder, Prepare.class, PrepareTask.class);
        bindDataDefinitionTask(binder, executionBinder, Deallocate.class, DeallocateTask.class);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }

    @Provides
    @Singleton
    public static QueryPerformanceFetcher createQueryPerformanceFetcher(QueryManager queryManager)
    {
        return queryManager::getFullQueryInfo;
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return InMemoryTransactionManager.create(config, idleCheckExecutor, catalogManager, finishingExecutor);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder,
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
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .add(transactionFinishingExecutor)
                    .add(transactionIdleExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
