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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.dispatcher.QueryPrerequisitesManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.server.security.PrestoAuthenticatorManager;
import com.facebook.presto.spi.ClientRequestFilterFactory;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.analyzer.QueryPreparerProvider;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.nodestatus.NodeStatusNotificationProviderFactory;
import com.facebook.presto.spi.plan.PlanCheckerProviderFactory;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.facebook.presto.spi.session.WorkerSessionPropertyProviderFactory;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spi.tracing.TracerProvider;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;
import com.facebook.presto.spi.tvf.TVFProviderFactory;
import com.facebook.presto.spi.type.TypeManagerFactory;
import com.facebook.presto.sql.analyzer.AnalyzerProviderManager;
import com.facebook.presto.sql.analyzer.QueryPreparerProviderManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.tracing.TracerProviderManager;
import com.facebook.presto.ttl.clusterttlprovidermanagers.ClusterTtlProviderManager;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.resolver.ArtifactResolver;
import jakarta.inject.Inject;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
{
    //  TODO: To make CoordinatorPlugin loading compulsory when native execution is enabled.
    private static final String COORDINATOR_PLUGIN_SERVICES_FILE = "META-INF/services/" + CoordinatorPlugin.class.getName();
    private static final String PLUGIN_SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();
    private static final Logger log = Logger.get(PluginManager.class);
    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final PrestoAuthenticatorManager prestoAuthenticatorManager;
    private final EventListenerManager eventListenerManager;
    private final BlockEncodingManager blockEncodingManager;
    private final TempStorageManager tempStorageManager;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final QueryPrerequisitesManager queryPrerequisitesManager;
    private final NodeTtlFetcherManager nodeTtlFetcherManager;
    private final ClusterTtlProviderManager clusterTtlProviderManager;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final ImmutableSet<String> disabledConnectors;
    private final HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager;
    private final TracerProviderManager tracerProviderManager;
    private final AnalyzerProviderManager analyzerProviderManager;
    private final QueryPreparerProviderManager queryPreparerProviderManager;
    private final NodeStatusNotificationManager nodeStatusNotificationManager;
    private final ClientRequestFilterManager clientRequestFilterManager;
    private final PlanCheckerProviderManager planCheckerProviderManager;
    private final ExpressionOptimizerManager expressionOptimizerManager;
    private final PluginInstaller pluginInstaller;

    @Inject
    public PluginManager(
            NodeInfo nodeInfo,
            PluginManagerConfig config,
            ConnectorManager connectorManager,
            Metadata metadata,
            ResourceGroupManager<?> resourceGroupManager,
            AnalyzerProviderManager analyzerProviderManager,
            QueryPreparerProviderManager queryPreparerProviderManager,
            AccessControlManager accessControlManager,
            PasswordAuthenticatorManager passwordAuthenticatorManager,
            PrestoAuthenticatorManager prestoAuthenticatorManager,
            EventListenerManager eventListenerManager,
            BlockEncodingManager blockEncodingManager,
            TempStorageManager tempStorageManager,
            QueryPrerequisitesManager queryPrerequisitesManager,
            SessionPropertyDefaults sessionPropertyDefaults,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            ClusterTtlProviderManager clusterTtlProviderManager,
            HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager,
            TracerProviderManager tracerProviderManager,
            NodeStatusNotificationManager nodeStatusNotificationManager,
            ClientRequestFilterManager clientRequestFilterManager,
            PlanCheckerProviderManager planCheckerProviderManager,
            ExpressionOptimizerManager expressionOptimizerManager)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");

        installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.prestoAuthenticatorManager = requireNonNull(prestoAuthenticatorManager, "prestoAuthenticatorManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        this.queryPrerequisitesManager = requireNonNull(queryPrerequisitesManager, "queryPrerequisitesManager is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.nodeTtlFetcherManager = requireNonNull(nodeTtlFetcherManager, "nodeTtlFetcherManager is null");
        this.clusterTtlProviderManager = requireNonNull(clusterTtlProviderManager, "clusterTtlProviderManager is null");
        this.disabledConnectors = requireNonNull(config.getDisabledConnectors(), "disabledConnectors is null");
        this.historyBasedPlanStatisticsManager = requireNonNull(historyBasedPlanStatisticsManager, "historyBasedPlanStatisticsManager is null");
        this.tracerProviderManager = requireNonNull(tracerProviderManager, "tracerProviderManager is null");
        this.analyzerProviderManager = requireNonNull(analyzerProviderManager, "analyzerProviderManager is null");
        this.queryPreparerProviderManager = requireNonNull(queryPreparerProviderManager, "queryPreparerProviderManager is null");
        this.nodeStatusNotificationManager = requireNonNull(nodeStatusNotificationManager, "nodeStatusNotificationManager is null");
        this.clientRequestFilterManager = requireNonNull(clientRequestFilterManager, "clientRequestFilterManager is null");
        this.planCheckerProviderManager = requireNonNull(planCheckerProviderManager, "planCheckerProviderManager is null");
        this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionManager is null");
        this.pluginInstaller = new MainPluginInstaller(this);
    }

    public void loadPlugins()
            throws Exception
    {
        PluginManagerUtil.loadPlugins(
                pluginsLoading,
                pluginsLoaded,
                installedPluginsDir,
                plugins,
                metadata,
                resolver,
                SPI_PACKAGES,
                COORDINATOR_PLUGIN_SERVICES_FILE,
                PLUGIN_SERVICES_FILE,
                pluginInstaller,
                getClass().getClassLoader());
    }

    public void installPlugin(Plugin plugin)
    {
        for (BlockEncoding blockEncoding : plugin.getBlockEncodings()) {
            log.info("Registering block encoding %s", blockEncoding.getName());
            blockEncodingManager.addBlockEncoding(blockEncoding);
        }

        for (Type type : plugin.getTypes()) {
            log.info("Registering type %s", type.getTypeSignature());
            metadata.getFunctionAndTypeManager().addType(type);
        }

        for (ParametricType parametricType : plugin.getParametricTypes()) {
            log.info("Registering parametric type %s", parametricType.getName());
            metadata.getFunctionAndTypeManager().addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            if (disabledConnectors.contains(connectorFactory.getName())) {
                log.info("Skipping disabled connector %s", connectorFactory.getName());
                continue;
            }
            log.info("Registering connector %s", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory);
        }

        for (Class<?> functionClass : plugin.getFunctions()) {
            log.info("Registering functions from %s", functionClass.getName());
            metadata.registerBuiltInFunctions(extractFunctions(functionClass));
        }

        for (FunctionNamespaceManagerFactory functionNamespaceManagerFactory : plugin.getFunctionNamespaceManagerFactories()) {
            log.info("Registering function namespace manager %s", functionNamespaceManagerFactory.getName());
            metadata.getFunctionAndTypeManager().addFunctionNamespaceFactory(functionNamespaceManagerFactory);
        }

        for (SessionPropertyConfigurationManagerFactory sessionConfigFactory : plugin.getSessionPropertyConfigurationManagerFactories()) {
            log.info("Registering session property configuration manager %s", sessionConfigFactory.getName());
            sessionPropertyDefaults.addConfigurationManagerFactory(sessionConfigFactory);
        }

        for (ResourceGroupConfigurationManagerFactory configurationManagerFactory : plugin.getResourceGroupConfigurationManagerFactories()) {
            log.info("Registering resource group configuration manager %s", configurationManagerFactory.getName());
            resourceGroupManager.addConfigurationManagerFactory(configurationManagerFactory);
        }

        for (SystemAccessControlFactory accessControlFactory : plugin.getSystemAccessControlFactories()) {
            log.info("Registering system access control %s", accessControlFactory.getName());
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        }

        for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
            log.info("Registering password authenticator %s", authenticatorFactory.getName());
            passwordAuthenticatorManager.addPasswordAuthenticatorFactory(authenticatorFactory);
        }

        for (PrestoAuthenticatorFactory authenticatorFactory : plugin.getPrestoAuthenticatorFactories()) {
            log.info("Registering presto authenticator %s", authenticatorFactory.getName());
            prestoAuthenticatorManager.addPrestoAuthenticatorFactory(authenticatorFactory);
        }

        for (EventListenerFactory eventListenerFactory : plugin.getEventListenerFactories()) {
            log.info("Registering event listener %s", eventListenerFactory.getName());
            eventListenerManager.addEventListenerFactory(eventListenerFactory);
        }

        for (TempStorageFactory tempStorageFactory : plugin.getTempStorageFactories()) {
            log.info("Registering temp storage %s", tempStorageFactory.getName());
            tempStorageManager.addTempStorageFactory(tempStorageFactory);
        }

        for (QueryPrerequisitesFactory queryPrerequisitesFactory : plugin.getQueryPrerequisitesFactories()) {
            log.info("Registering query prerequisite factory %s", queryPrerequisitesFactory.getName());
            queryPrerequisitesManager.addQueryPrerequisitesFactory(queryPrerequisitesFactory);
        }

        for (NodeTtlFetcherFactory nodeTtlFetcherFactory : plugin.getNodeTtlFetcherFactories()) {
            log.info("Registering Ttl fetcher factory %s", nodeTtlFetcherFactory.getName());
            nodeTtlFetcherManager.addNodeTtlFetcherFactory(nodeTtlFetcherFactory);
        }

        for (ClusterTtlProviderFactory clusterTtlProviderFactory : plugin.getClusterTtlProviderFactories()) {
            log.info("Registering Cluster Ttl provider factory %s", clusterTtlProviderFactory.getName());
            clusterTtlProviderManager.addClusterTtlProviderFactory(clusterTtlProviderFactory);
        }

        for (HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider : plugin.getHistoryBasedPlanStatisticsProviders()) {
            log.info("Registering plan statistics provider %s", historyBasedPlanStatisticsProvider.getName());
            historyBasedPlanStatisticsManager.addHistoryBasedPlanStatisticsProviderFactory(historyBasedPlanStatisticsProvider);
        }

        for (TracerProvider tracerProvider : plugin.getTracerProviders()) {
            log.info("Registering tracer provider %s", tracerProvider.getName());
            tracerProviderManager.addTracerProviderFactory(tracerProvider);
        }

        for (AnalyzerProvider analyzerProvider : plugin.getAnalyzerProviders()) {
            log.info("Registering analyzer provider %s", analyzerProvider.getType());
            analyzerProviderManager.addAnalyzerProvider(analyzerProvider);
        }

        for (QueryPreparerProvider preparerProvider : plugin.getQueryPreparerProviders()) {
            log.info("Registering query preparer provider %s", preparerProvider.getType());
            queryPreparerProviderManager.addQueryPreparerProvider(preparerProvider);
        }

        for (NodeStatusNotificationProviderFactory nodeStatusNotificationProviderFactory : plugin.getNodeStatusNotificationProviderFactory()) {
            log.info("Registering node status notification provider %s", nodeStatusNotificationProviderFactory.getName());
            nodeStatusNotificationManager.addNodeStatusNotificationProviderFactory(nodeStatusNotificationProviderFactory);
        }

        for (ClientRequestFilterFactory clientRequestFilterFactory : plugin.getClientRequestFilterFactories()) {
            log.info("Registering client request filter factory");
            clientRequestFilterManager.registerClientRequestFilterFactory(clientRequestFilterFactory);
        }

        for (Class<?> functionClass : plugin.getSqlInvokedFunctions()) {
            log.info("Registering functions from %s", functionClass.getName());
            metadata.getFunctionAndTypeManager().registerPluginFunctions(
                    extractFunctions(functionClass, metadata.getFunctionAndTypeManager().getDefaultNamespace()));
        }
    }

    public void installCoordinatorPlugin(CoordinatorPlugin plugin)
    {
        for (FunctionNamespaceManagerFactory functionNamespaceManagerFactory : plugin.getFunctionNamespaceManagerFactories()) {
            log.info("Registering function namespace manager %s", functionNamespaceManagerFactory.getName());
            metadata.getFunctionAndTypeManager().addFunctionNamespaceFactory(functionNamespaceManagerFactory);
        }

        for (WorkerSessionPropertyProviderFactory providerFactory : plugin.getWorkerSessionPropertyProviderFactories()) {
            log.info("Registering system session property provider factory %s", providerFactory.getName());
            metadata.getSessionPropertyManager().addSessionPropertyProviderFactory(providerFactory);
        }

        for (PlanCheckerProviderFactory planCheckerProviderFactory : plugin.getPlanCheckerProviderFactories()) {
            log.info("Registering plan checker provider factory %s", planCheckerProviderFactory.getName());
            planCheckerProviderManager.addPlanCheckerProviderFactory(planCheckerProviderFactory);
        }

        for (ExpressionOptimizerFactory expressionOptimizerFactory : plugin.getExpressionOptimizerFactories()) {
            log.info("Registering expression optimizer factory %s", expressionOptimizerFactory.getName());
            expressionOptimizerManager.addExpressionOptimizerFactory(expressionOptimizerFactory);
        }

        for (TypeManagerFactory typeManagerFactory : plugin.getTypeManagerFactories()) {
            log.info("Registering type manager factory %s", typeManagerFactory.getName());
            metadata.getFunctionAndTypeManager().addTypeManagerFactory(typeManagerFactory);
        }

        for (TVFProviderFactory tvfProviderFactory : plugin.getTVFProviderFactories()) {
            log.info("Registering table functions provider factory %s", tvfProviderFactory.getName());
            metadata.getFunctionAndTypeManager().addTVFProviderFactory(tvfProviderFactory);
        }
    }

    private class MainPluginInstaller
            implements PluginInstaller
    {
        private final PluginManager pluginManager;

        public MainPluginInstaller(PluginManager pluginManager)
        {
            this.pluginManager = pluginManager;
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
            pluginManager.installPlugin(plugin);
        }

        @Override
        public void installCoordinatorPlugin(CoordinatorPlugin plugin)
        {
            pluginManager.installCoordinatorPlugin(plugin);
        }
    }
}
