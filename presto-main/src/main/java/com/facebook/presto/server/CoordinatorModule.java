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
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.server.HttpServerBinder.HttpResourceBinding;
import com.facebook.airlift.units.Duration;
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
import com.facebook.presto.dispatcher.FailedLocalDispatchQueryFactory;
import com.facebook.presto.dispatcher.LocalDispatchQueryFactory;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.event.QueryProgressMonitor;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.EagerPlanValidationExecutionMBean;
import com.facebook.presto.execution.ExecutionFactoriesManager;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.ForEagerPlanValidation;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.ForTimeoutThread;
import com.facebook.presto.execution.NodeResourceStatusConfig;
import com.facebook.presto.execution.PartialResultQueryManager;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecutionMBean;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.AdaptivePhasedExecutionPolicy;
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
import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.server.protocol.ExecutingQueryResponseProvider;
import com.facebook.presto.server.protocol.ExecutingStatementResource;
import com.facebook.presto.server.protocol.LocalExecutingQueryResponseProvider;
import com.facebook.presto.server.protocol.LocalQueryProvider;
import com.facebook.presto.server.protocol.QueryBlockingRateLimiter;
import com.facebook.presto.server.protocol.QueuedStatementResource;
import com.facebook.presto.server.protocol.RetryCircuitBreaker;
import com.facebook.presto.server.remotetask.HttpRemoteTaskFactory;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClient;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClientConfig;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.transaction.ForTransactionManager;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.util.PrestoDataDefBindingHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.server.HttpServerBinder.httpServerBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.execution.AccessControlCheckerExecution.AccessControlCheckerExecutionFactory;
import static com.facebook.presto.execution.DDLDefinitionExecution.DDLDefinitionExecutionFactory;
import static com.facebook.presto.execution.SessionDefinitionExecution.SessionDefinitionExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    private static final String DEFAULT_WEBUI_CSP =
            "default-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
                    "font-src 'self' https://fonts.gstatic.com; frame-ancestors 'self'; img-src http: https: data:";

    public static HttpResourceBinding webUIBinder(Binder binder, String path, String classPathResourceBase)
    {
        return httpServerBinder(binder).bindResource(path, classPathResourceBase)
                .withExtraHeader(HttpHeaders.X_CONTENT_TYPE_OPTIONS, "nosniff")
                .withExtraHeader(HttpHeaders.CONTENT_SECURITY_POLICY, DEFAULT_WEBUI_CSP);
    }

    @Override
    protected void setup(Binder binder)
    {
        webUIBinder(binder, "/ui/dev", "webapp/dev").withWelcomeFile("index.html");
        webUIBinder(binder, "/ui", "webapp").withWelcomeFile("index.html");
        webUIBinder(binder, "/tableau", "webapp/tableau");

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
        binder.bind(QueryProgressMonitor.class).in(Scopes.SINGLETON);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();

        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(RetryCircuitBreaker.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RetryCircuitBreaker.class).withGeneratedName();

        binder.bind(QueryBlockingRateLimiter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryBlockingRateLimiter.class).withGeneratedName();

        // retry configuration
        configBinder(binder).bindConfig(RetryConfig.class);
        binder.bind(RetryUrlValidator.class).in(Scopes.SINGLETON);

        binder.bind(LocalQueryProvider.class).in(Scopes.SINGLETON);
        binder.bind(ExecutingQueryResponseProvider.class).to(LocalExecutingQueryResponseProvider.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(TaskInfoResource.class);

        // dispatcher
        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DispatchManager.class).withGeneratedName();
        binder.bind(FailedDispatchQueryFactory.class).to(FailedLocalDispatchQueryFactory.class);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DispatchExecutor.class).withGeneratedName();

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

        ReactorNettyHttpClientConfig reactorNettyHttpClientConfig = buildConfigObject(ReactorNettyHttpClientConfig.class);
        if (reactorNettyHttpClientConfig.isReactorNettyHttpClientEnabled()) {
            binder.bind(ReactorNettyHttpClient.class).in(Scopes.SINGLETON);
            binder.bind(HttpClient.class).annotatedWith(ForScheduler.class).to(ReactorNettyHttpClient.class);
        }
        else {
            httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                    .withTracing()
                    .withFilter(GenerateTraceTokenRequestFilter.class)
                    .withConfigDefaults(config -> {
                        config.setRequestTimeout(new Duration(10, SECONDS));
                        config.setMaxConnectionsPerServer(250);
                    });
        }

        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        // query execution
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class).as(generatedNameOf(QueryExecution.class));
        binder.bind(EagerPlanValidationExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(EagerPlanValidationExecutionMBean.class).withGeneratedName();

        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();
        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(SectionExecutionFactory.class).in(Scopes.SINGLETON);

        binder.bind(PartialResultQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(DDLDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(SessionDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(AccessControlCheckerExecutionFactory.class).in(Scopes.SINGLETON);
        binder.bind(ExecutionFactoriesManager.class).in(Scopes.SINGLETON);

        // helper class binding data definition tasks and statements
        PrestoDataDefBindingHelper.bindDDLDefinitionTasks(binder);
        PrestoDataDefBindingHelper.bindTransactionControlDefinitionTasks(binder);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);
        executionPolicyBinder.addBinding("adaptive").to(AdaptivePhasedExecutionPolicy.class);

        configBinder(binder).bindConfig(NodeResourceStatusConfig.class);
        binder.bind(NodeResourceStatusProvider.class).to(NodeResourceStatus.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, ResourceManagerProxy.class);
        install(installModuleIf(
                ServerConfig.class,
                ServerConfig::isResourceManagerEnabled,
                rmBinder -> {
                    httpClientBinder(rmBinder).bindHttpClient("resourceManager", ForResourceManager.class);
                    rmBinder.bind(ResourceManagerProxy.class).in(Scopes.SINGLETON);
                }));

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

    @Provides
    @Singleton
    @ForTimeoutThread
    public static ScheduledExecutorService createTimeoutThreadExecutor()
    {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("thread-timeout"));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Provides
    @Singleton
    @ForEagerPlanValidation
    public static ExecutorService createEagerPlanValidationExecutor(FeaturesConfig featuresConfig)
    {
        return new ThreadPoolExecutor(0, featuresConfig.getEagerPlanValidationThreadPoolSize(), 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), threadsNamed("plan-validation-%s"));
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
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor,
                @ForEagerPlanValidation ExecutorService eagerPlanValidationExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .add(transactionFinishingExecutor)
                    .add(transactionIdleExecutor)
                    .add(eagerPlanValidationExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
