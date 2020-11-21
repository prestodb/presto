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
package com.facebook.presto.server.testing;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.DiscoveryModule;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.discovery.client.ServiceSelectorManager;
import com.facebook.airlift.discovery.client.testing.TestingDiscoveryModule;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jmx.testing.TestingJmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.airlift.tracetoken.TraceTokenModule;
import com.facebook.drift.server.DriftServer;
import com.facebook.drift.transport.netty.server.DriftNettyServerTransport;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.ShutdownAction;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.smile.SmileModule;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.testing.ProcedureTester;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingEventListenerManager;
import com.facebook.presto.testing.TestingTempStorageManager;
import com.facebook.presto.testing.TestingWarningCollectorModule;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.MBeanModule;

import javax.annotation.concurrent.GuardedBy;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Integer.parseInt;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingPrestoServer
        implements Closeable
{
    private final Injector injector;
    private final Path baseDataDir;
    private final boolean preserveData;
    private final LifeCycleManager lifeCycleManager;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;
    private final TestingHttpServer server;
    private final CatalogManager catalogManager;
    private final TransactionManager transactionManager;
    private final SqlParser sqlParser;
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final TestingEventListenerManager eventListenerManager;
    private final TestingAccessControlManager accessControl;
    private final ProcedureTester procedureTester;
    private final Optional<InternalResourceGroupManager<?>> resourceGroupManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager planOptimizerManager;
    private final ClusterMemoryManager clusterMemoryManager;
    private final LocalMemoryManager localMemoryManager;
    private final InternalNodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;
    private final Announcer announcer;
    private final DispatchManager dispatchManager;
    private final SqlQueryManager queryManager;
    private final TaskManager taskManager;
    private final GracefulShutdownHandler gracefulShutdownHandler;
    private final ShutdownAction shutdownAction;
    private final RequestBlocker requestBlocker;
    private final boolean coordinator;

    public static class TestShutdownAction
            implements ShutdownAction
    {
        private final CountDownLatch shutdownCalled = new CountDownLatch(1);

        @GuardedBy("this")
        private boolean isWorkerShutdown;

        @Override
        public synchronized void onShutdown()
        {
            isWorkerShutdown = true;
            shutdownCalled.countDown();
        }

        public void waitForShutdownComplete(long millis)
                throws InterruptedException
        {
            shutdownCalled.await(millis, MILLISECONDS);
        }

        public synchronized boolean isWorkerShutdown()
        {
            return isWorkerShutdown;
        }
    }

    public TestingPrestoServer()
            throws Exception
    {
        this(ImmutableList.of());
    }

    public TestingPrestoServer(List<Module> additionalModules)
            throws Exception
    {
        this(true, ImmutableMap.of(), null, null, new SqlParserOptions(), additionalModules);
    }

    public TestingPrestoServer(
            boolean coordinator,
            Map<String, String> properties,
            String environment,
            URI discoveryUri,
            SqlParserOptions parserOptions,
            List<Module> additionalModules)
            throws Exception
    {
        this(coordinator, properties, environment, discoveryUri, parserOptions, additionalModules, Optional.empty());
    }

    public TestingPrestoServer(
            boolean coordinator,
            Map<String, String> properties,
            String environment,
            URI discoveryUri,
            SqlParserOptions parserOptions,
            List<Module> additionalModules,
            Optional<Path> baseDataDir)
            throws Exception
    {
        this.coordinator = coordinator;

        this.baseDataDir = baseDataDir.orElseGet(TestingPrestoServer::tempDirectory);
        this.preserveData = baseDataDir.isPresent();

        properties = new HashMap<>(properties);
        String coordinatorPort = properties.remove("http-server.http.port");
        if (coordinatorPort == null) {
            coordinatorPort = "0";
        }

        Map<String, String> serverProperties = getServerProperties(coordinator, properties, environment, discoveryUri);

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(Optional.ofNullable(environment)))
                .add(new TestingHttpServerModule(parseInt(coordinator ? coordinatorPort : "0")))
                .add(new JsonModule())
                .add(installModuleIf(
                        FeaturesConfig.class,
                        FeaturesConfig::isJsonSerdeCodeGenerationEnabled,
                        binder -> jsonBinder(binder).addModuleBinding().to(AfterburnerModule.class)))
                .add(new SmileModule())
                .add(new JaxrsModule(true))
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new ServerMainModule(parserOptions))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingTempStorageManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(TempStorageManager.class).to(TestingTempStorageManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                    binder.bind(RequestBlocker.class).in(Scopes.SINGLETON);
                    newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                            .to(RequestBlocker.class).in(Scopes.SINGLETON);
                });

        if (discoveryUri != null) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            modules.add(new DiscoveryModule());
        }
        else {
            modules.add(new TestingDiscoveryModule());
        }

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());

        Map<String, String> optionalProperties = new HashMap<>();
        if (environment != null) {
            optionalProperties.put("node.environment", environment);
        }

        injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties)
                .setOptionalConfigurationProperties(optionalProperties)
                .quiet()
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        pluginManager = injector.getInstance(PluginManager.class);

        connectorManager = injector.getInstance(ConnectorManager.class);

        server = injector.getInstance(TestingHttpServer.class);
        catalogManager = injector.getInstance(CatalogManager.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        sqlParser = injector.getInstance(SqlParser.class);
        metadata = injector.getInstance(Metadata.class);
        accessControl = injector.getInstance(TestingAccessControlManager.class);
        procedureTester = injector.getInstance(ProcedureTester.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        if (coordinator) {
            dispatchManager = injector.getInstance(DispatchManager.class);
            queryManager = (SqlQueryManager) injector.getInstance(QueryManager.class);
            resourceGroupManager = Optional.of(injector.getInstance(InternalResourceGroupManager.class));
            nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
            planOptimizerManager = injector.getInstance(ConnectorPlanOptimizerManager.class);
            clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
            statsCalculator = injector.getInstance(StatsCalculator.class);
            eventListenerManager = ((TestingEventListenerManager) injector.getInstance(EventListenerManager.class));
        }
        else {
            dispatchManager = null;
            queryManager = null;
            resourceGroupManager = Optional.empty();
            nodePartitioningManager = null;
            planOptimizerManager = null;
            clusterMemoryManager = null;
            statsCalculator = null;
            eventListenerManager = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        gracefulShutdownHandler = injector.getInstance(GracefulShutdownHandler.class);
        taskManager = injector.getInstance(TaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        announcer = injector.getInstance(Announcer.class);
        requestBlocker = injector.getInstance(RequestBlocker.class);

        // Announce Thrift server address
        DriftServer driftServer = injector.getInstance(DriftServer.class);
        driftServer.start();
        updateThriftServerAddressAnnouncement(announcer, driftServerPort(driftServer), nodeManager);

        announcer.forceAnnounce();

        refreshNodes();
    }

    private Map<String, String> getServerProperties(boolean coordinator, Map<String, String> properties, String environment, URI discoveryUri)
    {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put("coordinator", String.valueOf(coordinator));
        serverProperties.put("presto.version", "testversion");
        serverProperties.put("task.concurrency", "4");
        serverProperties.put("task.max-worker-threads", "4");
        serverProperties.put("exchange.client-threads", "4");
        serverProperties.put("optimizer.ignore-stats-calculator-failures", "false");
        if (coordinator) {
            // enabling failure detector in tests can make them flakey
            serverProperties.put("failure-detector.enabled", "false");
        }

        if (discoveryUri != null) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.toString());
        }
        // Add these last so explicitly specified properties override the defaults
        serverProperties.putAll(properties);
        return ImmutableMap.copyOf(serverProperties);
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            if (isDirectory(baseDataDir) && !preserveData) {
                deleteRecursively(baseDataDir, ALLOW_INSECURE);
            }
        }
    }

    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    public DispatchManager getDispatchManager()
    {
        return dispatchManager;
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryManager.getQueryPlan(queryId);
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryManager.addFinalQueryInfoListener(queryId, stateChangeListener);
    }

    public ConnectorId createCatalog(String catalogName, String connectorName)
    {
        return createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public ConnectorId createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        ConnectorId connectorId = connectorManager.createConnection(catalogName, connectorName, properties);
        updateConnectorIdAnnouncement(announcer, connectorId, nodeManager);
        return connectorId;
    }

    public Path getBaseDataDir()
    {
        return baseDataDir;
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(getBaseUrl().getHost(), getBaseUrl().getPort());
    }

    public HostAndPort getHttpsAddress()
    {
        URI httpsUri = server.getHttpServerInfo().getHttpsUri();
        return HostAndPort.fromParts(httpsUri.getHost(), httpsUri.getPort());
    }

    public CatalogManager getCatalogManager()
    {
        return catalogManager;
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public SqlParser getSqlParser()
    {
        return sqlParser;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public StatsCalculator getStatsCalculator()
    {
        checkState(coordinator, "not a coordinator");
        return statsCalculator;
    }

    public Optional<EventListener> getEventListener()
    {
        checkState(coordinator, "not a coordinator");
        return eventListenerManager.getEventListener();
    }

    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public ProcedureTester getProcedureTester()
    {
        return procedureTester;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public Optional<InternalResourceGroupManager<?>> getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return planOptimizerManager;
    }

    public LocalMemoryManager getLocalMemoryManager()
    {
        return localMemoryManager;
    }

    public ClusterMemoryManager getClusterMemoryManager()
    {
        checkState(coordinator, "not a coordinator");
        return clusterMemoryManager;
    }

    public GracefulShutdownHandler getGracefulShutdownHandler()
    {
        return gracefulShutdownHandler;
    }

    public TaskManager getTaskManager()
    {
        return taskManager;
    }

    public ShutdownAction getShutdownAction()
    {
        return shutdownAction;
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    public final AllNodes refreshNodes()
    {
        serviceSelectorManager.forceRefresh();
        nodeManager.refreshNodes();
        return nodeManager.getAllNodes();
    }

    public Set<InternalNode> getActiveNodesWithConnector(ConnectorId connectorId)
    {
        return nodeManager.getActiveConnectorNodes(connectorId);
    }

    public <T> T getInstance(Key<T> key)
    {
        return injector.getInstance(key);
    }

    public void stopResponding()
    {
        requestBlocker.block();
    }

    public void startResponding()
    {
        requestBlocker.unblock();
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, ConnectorId connectorId, InternalNodeManager nodeManager)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    // TODO: announcement does not work for coordinator
    private static void updateThriftServerAddressAnnouncement(Announcer announcer, int thriftPort, InternalNodeManager nodeManager)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update announcement and thrift port property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        properties.put("thriftServerPort", String.valueOf(thriftPort));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }

    private static Path tempDirectory()
    {
        try {
            return createTempDirectory("PrestoTest");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class RequestBlocker
            implements Filter
    {
        private static final Object monitor = new Object();
        private volatile boolean blocked;

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException
        {
            synchronized (monitor) {
                while (blocked) {
                    try {
                        monitor.wait();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
            }
            chain.doFilter(request, response);
        }

        public void block()
        {
            synchronized (monitor) {
                blocked = true;
            }
        }

        public void unblock()
        {
            synchronized (monitor) {
                blocked = false;
                monitor.notifyAll();
            }
        }

        @Override
        public void init(FilterConfig filterConfig) {}

        @Override
        public void destroy() {}
    }

    private static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }
}
