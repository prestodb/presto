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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
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
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.facebook.presto.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("org.openjdk.jol.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private final ConnectorManager connectorManager;
    private final Metadata metadata;
    private final ResourceGroupManager resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final EventListenerManager eventListenerManager;
    private final BlockEncodingManager blockEncodingManager;
    private final TypeRegistry typeRegistry;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(
            NodeInfo nodeInfo,
            HttpServerInfo httpServerInfo,
            PluginManagerConfig config,
            ConnectorManager connectorManager,
            ConfigurationFactory configurationFactory,
            Metadata metadata,
            ResourceGroupManager resourceGroupManager,
            AccessControlManager accessControlManager,
            EventListenerManager eventListenerManager,
            BlockEncodingManager blockEncodingManager,
            TypeRegistry typeRegistry)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(httpServerInfo, "httpServerInfo is null");
        requireNonNull(config, "config is null");
        requireNonNull(configurationFactory, "configurationFactory is null");

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
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
    }

    public boolean arePluginsLoaded()
    {
        return pluginsLoaded.get();
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
            throws Exception
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
        for (BlockEncodingFactory<?> blockEncodingFactory : plugin.getBlockEncodingFactories(blockEncodingManager)) {
            log.info("Registering block encoding %s", blockEncodingFactory.getName());
            blockEncodingManager.addBlockEncodingFactory(blockEncodingFactory);
        }

        for (Type type : plugin.getTypes()) {
            log.info("Registering type %s", type.getTypeSignature());
            typeRegistry.addType(type);
        }

        for (ParametricType parametricType : plugin.getParametricTypes()) {
            log.info("Registering parametric type %s", parametricType.getName());
            typeRegistry.addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory);
        }

        for (Class<?> functionClass : plugin.getFunctions()) {
            log.info("Registering functions from %s", functionClass.getName());
            metadata.addFunctions(extractFunctions(functionClass));
        }

        for (ResourceGroupConfigurationManagerFactory configurationManagerFactory : plugin.getResourceGroupConfigurationManagerFactories()) {
            log.info("Registering resource group configuration manager %s", configurationManagerFactory.getName());
            resourceGroupManager.addConfigurationManagerFactory(configurationManagerFactory);
        }

        for (SystemAccessControlFactory accessControlFactory : plugin.getSystemAccessControlFactories()) {
            log.info("Registering system access control %s", accessControlFactory.getName());
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        }

        for (EventListenerFactory eventListenerFactory : plugin.getEventListenerFactories()) {
            log.info("Registering event listener %s", eventListenerFactory.getName());
            eventListenerManager.addEventListenerFactory(eventListenerFactory);
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
