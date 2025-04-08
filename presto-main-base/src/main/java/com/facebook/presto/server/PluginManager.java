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
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
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
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.server.PluginDiscovery.discoverPlugins;
import static com.facebook.presto.server.PluginDiscovery.writePluginServices;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
{
    // When generating code the AfterBurner module loads classes with *some* classloader.
    // When the AfterBurner module is configured not to use the value classloader
    // (e.g., AfterBurner().setUseValueClassLoader(false)) AppClassLoader is used for loading those
    // classes. Otherwise, the PluginClassLoader is used, which is the default behavior.
    // Therefore, in the former case Afterburner won't be able to load the connector classes
    // as AppClassLoader doesn't see them, and in the latter case the PluginClassLoader won't be
    // able to load the AfterBurner classes themselves. So, our solution is to use the PluginClassLoader
    // and whitelist the AfterBurner classes here, so that the PluginClassLoader can load the
    // AfterBurner classes.
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.facebook.presto.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("com.fasterxml.jackson.module.afterburner.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("org.openjdk.jol.")
            .add("com.facebook.presto.common")
            .add("com.facebook.drift.annotations.")
            .add("com.facebook.drift.TException")
            .add("com.facebook.drift.TApplicationException")
            .build();

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
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(installedPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(file.getAbsolutePath());
            }
        }

        for (String plugin : plugins) {
            loadPlugin(plugin);
        }

        metadata.verifyComparableOrderableContract();

        pluginsLoaded.set(true);
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader, CoordinatorPlugin.class);
            loadPlugin(pluginClassLoader, Plugin.class);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader, Class<?> clazz)
    {
        ServiceLoader<?> serviceLoader = ServiceLoader.load(clazz, pluginClassLoader);
        List<?> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", clazz.getName());
        }

        for (Object plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            if (plugin instanceof Plugin) {
                installPlugin((Plugin) plugin);
            }
            else if (plugin instanceof CoordinatorPlugin) {
                installCoordinatorPlugin((CoordinatorPlugin) plugin);
            }
            else {
                log.warn("Unknown plugin type: %s", plugin.getClass().getName());
            }
        }
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

            if (connectorFactory.getTableFunctionProcessorProvider().isPresent()) {
                metadata.getFunctionAndTypeManager().setGetTableFunctionProcessorProvider(connectorFactory.getTableFunctionProcessorProvider());
            }
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
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws Exception
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        return buildClassLoaderFromCoordinates(plugin);
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        URLClassLoader classLoader = createClassLoader(artifacts, pomFile.getPath());

        Artifact artifact = artifacts.get(0);

        processPlugins(artifact, classLoader, COORDINATOR_PLUGIN_SERVICES_FILE, CoordinatorPlugin.class.getName());
        processPlugins(artifact, classLoader, PLUGIN_SERVICES_FILE, Plugin.class.getName());

        return classLoader;
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }

    private URLClassLoader createClassLoader(List<Artifact> artifacts, String name)
            throws IOException
    {
        log.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, SPI_PACKAGES);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = new ArrayList<>(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    private void processPlugins(Artifact artifact, ClassLoader classLoader, String servicesFile, String className)
            throws IOException
    {
        Set<String> plugins = discoverPlugins(artifact, classLoader, servicesFile, className);
        if (!plugins.isEmpty()) {
            writePluginServices(plugins, artifact.getFile(), servicesFile);
        }
    }
}
