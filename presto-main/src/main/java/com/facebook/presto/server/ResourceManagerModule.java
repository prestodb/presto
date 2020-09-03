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
import com.facebook.presto.dispatcher.DispatchExecutor;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.dispatcher.DispatchQueryFactory;
import com.facebook.presto.dispatcher.DispatcherConfig;
import com.facebook.presto.dispatcher.DistributedClusterMemoryManager;
import com.facebook.presto.dispatcher.FailedDispatchQueryFactory;
import com.facebook.presto.dispatcher.NoopQueryManager;
import com.facebook.presto.dispatcher.QueryInfoFetcherFactory;
import com.facebook.presto.dispatcher.RemoteDispatchQueryFactory;
import com.facebook.presto.dispatcher.RemoteQueryStats;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.memory.ForRemoteDispatch;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.server.protocol.QueryProvider;
import com.facebook.presto.server.protocol.QueuedStatementResource;
import com.facebook.presto.server.protocol.RedirectingQueryProvider;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.transaction.ForTransactionManager;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.server.HttpServerBinder.httpServerBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.server.smile.SmileCodecBinder.smileCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ResourceManagerModule
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
        discoveryBinder(binder).bindHttpAnnouncement("presto-resource-manager");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jaxrsBinder(binder).bind(QueuedStatementResource.class);

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

        // TODO: remove query manager from this module
        binder.bind(QueryManager.class).to(NoopQueryManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);

        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);

        binder.bind(QueryProvider.class).to(RedirectingQueryProvider.class).in(Scopes.SINGLETON);

        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        // dispatcher
        configBinder(binder).bindConfig(DispatcherConfig.class);
        binder.bind(RemoteDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(QueryInfoFetcherFactory.class).in(Scopes.SINGLETON);
        binder.bind(RemoteQueryStats.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(BasicQueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(BasicQueryInfo.class);

        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("remoteDispatch", ForRemoteDispatch.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        binder.bind(DispatchQueryFactory.class).to(RemoteDispatchQueryFactory.class);

        // TODO: cluster memory manager
        binder.bind(DistributedClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(DistributedClusterMemoryManager.class).in(Scopes.SINGLETON);

        // node monitor
        binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);

        // TODO: cluster statistics
//        jaxrsBinder(binder).bind(ClusterStatsResource.class);

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

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
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
