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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.RouterPlugin;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

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

import static com.facebook.presto.server.PluginDiscovery.discoverPlugins;
import static com.facebook.presto.server.PluginDiscovery.writePluginServices;

public class PluginManagerUtil
{
    private static final Logger log = Logger.get(PluginManagerUtil.class);

    /*
     When generating code the AfterBurner module loads classes with *some* classloader.
     When the AfterBurner module is configured not to use the value classloader
     (e.g., AfterBurner().setUseValueClassLoader(false)) AppClassLoader is used for loading those
     classes. Otherwise, the PluginClassLoader is used, which is the default behavior.
     Therefore, in the former case Afterburner won't be able to load the connector classes
     as AppClassLoader doesn't see them, and in the latter case the PluginClassLoader won't be
     able to load the AfterBurner classes themselves. So, our solution is to use the PluginClassLoader
     and whitelist the AfterBurner classes here, so that the PluginClassLoader can load the
     AfterBurner classes.
     */
    public static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
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

    private PluginManagerUtil()
    {
    }

    public static void loadPlugins(
            AtomicBoolean pluginsLoading,
            AtomicBoolean pluginsLoaded,
            File installedPluginsDir,
            List<String> plugins,
            Metadata metadata,
            ArtifactResolver resolver,
            List<String> spiPackages,
            String coordinatorPluginServicesFile,
            String pluginServicesFile,
            PluginInstaller pluginInstaller,
            ClassLoader parent)
            throws Exception
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(installedPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(
                        file.getAbsolutePath(),
                        resolver,
                        spiPackages,
                        coordinatorPluginServicesFile,
                        pluginServicesFile,
                        pluginInstaller,
                        parent);
            }
        }

        for (String plugin : plugins) {
            loadPlugin(
                    plugin,
                    resolver,
                    spiPackages,
                    coordinatorPluginServicesFile,
                    pluginServicesFile,
                    pluginInstaller,
                    parent);
        }

        if (metadata != null) {
            metadata.verifyComparableOrderableContract();
        }

        pluginsLoaded.set(true);
    }

    public static void loadPlugin(
            String plugin,
            ArtifactResolver resolver,
            List<String> spiPackages,
            String coordinatorPluginServicesFile,
            String pluginServicesFile,
            PluginInstaller pluginInstaller,
            ClassLoader parent)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(
                plugin,
                resolver,
                spiPackages,
                coordinatorPluginServicesFile,
                pluginServicesFile,
                parent);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader, RouterPlugin.class, pluginInstaller);
            loadPlugin(pluginClassLoader, CoordinatorPlugin.class, pluginInstaller);
            loadPlugin(pluginClassLoader, Plugin.class, pluginInstaller);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    public static void loadPlugin(
            URLClassLoader pluginClassLoader,
            Class<?> clazz,
            PluginInstaller pluginInstaller)
    {
        ServiceLoader<?> serviceLoader = ServiceLoader.load(clazz, pluginClassLoader);
        List<?> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", clazz.getName());
        }

        for (Object plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            if (plugin instanceof Plugin) {
                pluginInstaller.installPlugin((Plugin) plugin);
            }
            else if (plugin instanceof CoordinatorPlugin) {
                pluginInstaller.installCoordinatorPlugin((CoordinatorPlugin) plugin);
            }
            else if (plugin instanceof RouterPlugin) {
                pluginInstaller.installRouterPlugin((RouterPlugin) plugin);
            }
            else {
                log.warn("Unknown plugin type: %s", plugin.getClass().getName());
            }
        }
    }

    private static URLClassLoader buildClassLoader(
            String plugin,
            ArtifactResolver resolver,
            List<String> spiPackages,
            String coordinatorPluginServicesFile,
            String pluginServicesFile,
            ClassLoader parent)
            throws Exception
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file, resolver, spiPackages, coordinatorPluginServicesFile, pluginServicesFile, parent);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file, spiPackages, parent);
        }
        return buildClassLoaderFromCoordinates(plugin, resolver, spiPackages, parent);
    }

    private static URLClassLoader buildClassLoaderFromPom(
            File pomFile,
            ArtifactResolver resolver,
            List<String> spiPackages,
            String coordinatorPluginServicesFile,
            String pluginServicesFile,
            ClassLoader parent)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        URLClassLoader classLoader = createClassLoader(artifacts, pomFile.getPath(), spiPackages, parent);

        Artifact artifact = artifacts.get(0);

        processPlugins(artifact, classLoader, pluginServicesFile, Plugin.class.getName());
        if (coordinatorPluginServicesFile != null) {
            processPlugins(artifact, classLoader, coordinatorPluginServicesFile, CoordinatorPlugin.class.getName());
        }

        return classLoader;
    }

    private static URLClassLoader buildClassLoaderFromDirectory(File dir, List<String> spiPackages, ClassLoader parent)
            throws Exception
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls, spiPackages, parent);
    }

    private static URLClassLoader buildClassLoaderFromCoordinates(
            String coordinates,
            ArtifactResolver resolver,
            List<String> spiPackages,
            ClassLoader parent)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString(), spiPackages, parent);
    }

    private static URLClassLoader createClassLoader(
            List<Artifact> artifacts,
            String name,
            List<String> spiPackages,
            ClassLoader parent)
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
        return createClassLoader(urls, spiPackages, parent);
    }

    private static URLClassLoader createClassLoader(List<URL> urls, List<String> spiPackages, ClassLoader parent)
    {
        return new PluginClassLoader(urls, parent, spiPackages);
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

    private static void processPlugins(Artifact artifact, ClassLoader classLoader, String servicesFile, String className)
            throws IOException
    {
        Set<String> plugins = discoverPlugins(artifact, classLoader, servicesFile, className);
        if (!plugins.isEmpty()) {
            writePluginServices(plugins, artifact.getFile(), servicesFile);
        }
    }
}
