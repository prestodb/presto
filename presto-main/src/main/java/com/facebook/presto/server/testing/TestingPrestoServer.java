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

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.ShutdownAction;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.testing.ProcedureTester;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingEventListenerManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.facebook.presto.server.testing.FileUtils.deleteRecursively;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingPrestoServer
        implements Closeable
{
    private final Path baseDataDir;
    private final LifeCycleManager lifeCycleManager;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;
    private final TestingHttpServer server;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final TestingAccessControlManager accessControl;
    private final ProcedureTester procedureTester;
    private final Optional<ResourceGroupManager> resourceGroupManager;
    private final SplitManager splitManager;
    private final ClusterMemoryManager clusterMemoryManager;
    private final LocalMemoryManager localMemoryManager;
    private final InternalNodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;
    private final Announcer announcer;
    private final QueryManager queryManager;
    private final TaskManager taskManager;
    private final GracefulShutdownHandler gracefulShutdownHandler;
    private final ShutdownAction shutdownAction;
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
        this(ImmutableList.<Module>of());
    }

    public TestingPrestoServer(List<Module> additionalModules)
            throws Exception
    {
        this(true, ImmutableMap.<String, String>of(), null, null, additionalModules);
    }

    public TestingPrestoServer(boolean coordinator, Map<String, String> properties, String environment, URI discoveryUri, List<Module> additionalModules)
            throws Exception
    {
        this.coordinator = coordinator;
        baseDataDir = Files.createTempDirectory("PrestoTest");

        properties = new HashMap<>(properties);
        String coordinatorPort = properties.remove("http-server.http.port");
        if (coordinatorPort == null) {
            coordinatorPort = "0";
        }

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("coordinator", String.valueOf(coordinator))
                .put("presto.version", "testversion")
                .put("http-client.max-threads", "16")
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                .put("exchange.client-threads", "4")
                .put("analyzer.experimental-syntax-enabled", "true");

        if (!properties.containsKey("query.max-memory-per-node")) {
            serverProperties.put("query.max-memory-per-node", "512MB");
        }

        if (coordinator) {
            // TODO: enable failure detector
            serverProperties.put("failure-detector.enabled", "false");
        }

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(Optional.ofNullable(environment)))
                .add(new TestingHttpServerModule(parseInt(coordinator ? coordinatorPort : "0")))
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerMainModule(new SqlParserOptions()))
                .add(binder -> {
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                });

        if (discoveryUri != null) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.toString());
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

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties.build())
                .setOptionalConfigurationProperties(optionalProperties)
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        queryManager = injector.getInstance(QueryManager.class);

        pluginManager = injector.getInstance(PluginManager.class);

        connectorManager = injector.getInstance(ConnectorManager.class);

        server = injector.getInstance(TestingHttpServer.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        metadata = injector.getInstance(Metadata.class);
        accessControl = injector.getInstance(TestingAccessControlManager.class);
        procedureTester = injector.getInstance(ProcedureTester.class);
        splitManager = injector.getInstance(SplitManager.class);
        FeaturesConfig config = injector.getInstance(FeaturesConfig.class);
        if (config.isResourceGroupsEnabled()) {
            resourceGroupManager = Optional.of(injector.getInstance(ResourceGroupManager.class));
        }
        else {
            resourceGroupManager = Optional.empty();
        }
        if (coordinator) {
            clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
        }
        else {
            clusterMemoryManager = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        gracefulShutdownHandler = injector.getInstance(GracefulShutdownHandler.class);
        taskManager = injector.getInstance(TaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        announcer = injector.getInstance(Announcer.class);

        announcer.forceAnnounce();

        refreshNodes();
    }

    @Override
    public void close()
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        finally {
            deleteRecursively(baseDataDir);
        }
    }

    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.<String, String>of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        connectorManager.createConnection(catalogName, connectorName, properties);
        updateDatasourcesAnnouncement(announcer, catalogName);
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

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public Metadata getMetadata()
    {
        return metadata;
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

    public Optional<ResourceGroupManager> getResourceGroupManager()
    {
        return resourceGroupManager;
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

    public Set<Node> getActiveNodesWithConnector(String connectorName)
    {
        return nodeManager.getActiveDatasourceNodes(connectorName);
    }

    private static void updateDatasourcesAnnouncement(Announcer announcer, String connectorId)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the data source property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update datasources property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("datasources"));
        Set<String> datasources = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        datasources.add(connectorId);
        properties.put("datasources", Joiner.on(',').join(datasources));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
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
}
