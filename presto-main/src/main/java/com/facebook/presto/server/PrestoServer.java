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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.DiscoveryModule;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.event.client.HttpEventModule;
import com.facebook.airlift.event.client.JsonEventModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jmx.JmxHttpModule;
import com.facebook.airlift.jmx.JmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.airlift.log.LogJmxModule;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.node.NodeModule;
import com.facebook.airlift.tracetoken.TraceTokenModule;
import com.facebook.drift.server.DriftServer;
import com.facebook.drift.transport.netty.server.DriftNettyServerTransport;
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.ClientRequestFilterModule;
import com.facebook.presto.builtin.tools.WorkerFunctionRegistryTool;
import com.facebook.presto.dispatcher.QueryPrerequisitesManager;
import com.facebook.presto.dispatcher.QueryPrerequisitesManagerModule;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorModule;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.metadata.StaticTypeManagerStore;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.security.oauth2.OAuth2Client;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.storage.TempStorageModule;
import com.facebook.presto.tracing.TracerProviderManager;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ClusterTtlProviderManager;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ClusterTtlProviderManagerModule;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManagerModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.weakref.jmx.guice.MBeanModule;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static com.facebook.presto.server.security.SecurityConfig.AuthenticationType.OAUTH2;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public class PrestoServer
        implements Runnable
{
    public static void main(String[] args)
    {
        new PrestoServer().run();
    }

    private final SqlParserOptions sqlParserOptions;

    public PrestoServer()
    {
        this(new SqlParserOptions());
    }

    public PrestoServer(SqlParserOptions sqlParserOptions)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new ClientRequestFilterModule(),
                new JsonModule(),
                installModuleIf(
                        FeaturesConfig.class,
                        FeaturesConfig::isJsonSerdeCodeGenerationEnabled,
                        binder -> jsonBinder(binder).addModuleBinding().to(AfterburnerModule.class)),
                new SmileModule(),
                new JaxrsModule(true),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ServerMainModule(sqlParserOptions),
                new GracefulShutdownModule(),
                new WarningCollectorModule(),
                new TempStorageModule(),
                new QueryPrerequisitesManagerModule(),
                new NodeTtlFetcherManagerModule(),
                new ClusterTtlProviderManagerModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.initialize();

            injector.getInstance(PluginManager.class).loadPlugins();

            ServerConfig serverConfig = injector.getInstance(ServerConfig.class);

            if (!serverConfig.isResourceManager()) {
                injector.getInstance(StaticCatalogStore.class).loadCatalogs();
            }

            // TODO: remove this huge hack
            updateConnectorIds(
                    injector.getInstance(Announcer.class),
                    injector.getInstance(CatalogManager.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));

            // TODO: thrift server port should be announced by discovery server similar to http/https ports
            updateThriftServerPort(
                    injector.getInstance(Announcer.class),
                    injector.getInstance(DriftServer.class));

            injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers();
            injector.getInstance(StaticTypeManagerStore.class).loadTypeManagers();
            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            if (!serverConfig.isResourceManager()) {
                injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            }
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(PrestoAuthenticatorManager.class).loadPrestoAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListeners();
            injector.getInstance(TempStorageManager.class).loadTempStorages();
            injector.getInstance(QueryPrerequisitesManager.class).loadQueryPrerequisites();
            injector.getInstance(NodeTtlFetcherManager.class).loadNodeTtlFetcher();
            injector.getInstance(ClusterTtlProviderManager.class).loadClusterTtlProvider();
            injector.getInstance(TracerProviderManager.class).loadTracerProvider();
            injector.getInstance(NodeStatusNotificationManager.class).loadNodeStatusNotificationProvider();
            injector.getInstance(GracefulShutdownHandler.class).loadNodeStatusNotification();
            injector.getInstance(SessionPropertyManager.class).loadSessionPropertyProviders();
            PlanCheckerProviderManager planCheckerProviderManager = injector.getInstance(PlanCheckerProviderManager.class);
            InternalNodeManager nodeManager = injector.getInstance(DiscoveryNodeManager.class);
            NodeInfo nodeInfo = injector.getInstance(NodeInfo.class);
            PluginNodeManager pluginNodeManager = new PluginNodeManager(nodeManager, nodeInfo.getEnvironment());
            planCheckerProviderManager.loadPlanCheckerProviders(pluginNodeManager);
            injector.getInstance(FunctionAndTypeManager.class).loadTVFProviders(pluginNodeManager);

            if (injector.getInstance(FeaturesConfig.class).isBuiltInSidecarFunctionsEnabled()) {
                List<? extends SqlFunction> functions = injector.getInstance(WorkerFunctionRegistryTool.class).getWorkerFunctions();
                injector.getInstance(FunctionAndTypeManager.class).registerWorkerFunctions(functions);
            }

            injector.getInstance(ClientRequestFilterManager.class).loadClientRequestFilters();
            injector.getInstance(ExpressionOptimizerManager.class).loadExpressionOptimizerFactories();

            injector.getInstance(FunctionAndTypeManager.class)
                    .getBuiltInPluginFunctionNamespaceManager().triggerConflictCheckWithBuiltInFunctions();

            startAssociatedProcesses(injector);

            SecurityConfig securityConfig = injector.getInstance(SecurityConfig.class);
            if (securityConfig.getAuthenticationTypes().contains(OAUTH2)) {
                injector.getInstance(OAuth2Client.class).load();
            }

            injector.getInstance(Announcer.class).start();

            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    protected void startAssociatedProcesses(Injector injector)
    {
    }

    private static void updateConnectorIds(Announcer announcer, CatalogManager metadata, ServerConfig serverConfig, NodeSchedulerConfig schedulerConfig)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // get existing connectorIds
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);

        // automatically build connectorIds if not configured
        if (connectorIds.isEmpty()) {
            List<Catalog> catalogs = metadata.getCatalogs();
            // if this is a dedicated coordinator, only add jmx
            if (serverConfig.isCoordinator() && !schedulerConfig.isIncludeCoordinator()) {
                catalogs.stream()
                        .map(Catalog::getConnectorId)
                        .filter(connectorId -> connectorId.getCatalogName().equals("jmx"))
                        .map(Object::toString)
                        .forEach(connectorIds::add);
            }
            else {
                catalogs.stream()
                        .map(Catalog::getConnectorId)
                        .map(Object::toString)
                        .forEach(connectorIds::add);
            }
        }

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static void updateThriftServerPort(Announcer announcer, DriftServer driftServer)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // drift server::start can be called multiple times
        driftServer.start();

        // update announcement and thrift port property
        int thriftPort = ((DriftNettyServerTransport) driftServer.getServerTransport()).getPort();
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        properties.put("thriftServerPort", String.valueOf(thriftPort));
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
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }
}
