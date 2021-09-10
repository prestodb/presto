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
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.dispatcher.QueryPrerequisitesManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManagerFactory;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spi.ttl.ClusterTtlProviderFactory;
import com.facebook.presto.spi.ttl.NodeTtlFetcherFactory;
import com.facebook.presto.storage.TempStorageManager;
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

    private static final Logger log = Logger.get(PluginManager.class);

    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
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

    @Inject
    public PluginManager(
            NodeInfo nodeInfo,
            PluginManagerConfig config,
            ConnectorManager connectorManager,
            Metadata metadata,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControlManager accessControlManager,
            PasswordAuthenticatorManager passwordAuthenticatorManager,
            EventListenerManager eventListenerManager,
            BlockEncodingManager blockEncodingManager,
            TempStorageManager tempStorageManager,
            QueryPrerequisitesManager queryPrerequisitesManager,
            SessionPropertyDefaults sessionPropertyDefaults,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            ClusterTtlProviderManager clusterTtlProviderManager)
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
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        this.queryPrerequisitesManager = requireNonNull(queryPrerequisitesManager, "queryPrerequisitesManager is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.nodeTtlFetcherManager = requireNonNull(nodeTtlFetcherManager, "nodeTtlFetcherManager is null");
        this.clusterTtlProviderManager = requireNonNull(clusterTtlProviderManager, "clusterTtlProviderManager is null");
        this.disabledConnectors = requireNonNull(config.getDisabledConnectors(), "disabledConnectors is null");
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
            loadPlugin(pluginClassLoader);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", Plugin.class.getName());
        }

        for (Plugin plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            installPlugin(plugin);
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
        Set<String> plugins = discoverPlugins(artifact, classLoader);
        if (!plugins.isEmpty()) {
            writePluginServices(plugins, artifact.getFile());
        }

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
}
