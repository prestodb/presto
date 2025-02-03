package com.facebook.presto.router;
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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.server.PluginClassLoader;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.security.PasswordAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.facebook.presto.server.PluginDiscovery.discoverPlugins;
import static com.facebook.presto.server.PluginDiscovery.writePluginServices;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class RouterPluginManager
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
    private static final String SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();
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

    private static final Logger log = Logger.get(RouterPluginManager.class);

    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public RouterPluginManager(
            PluginManagerConfig config,
            PasswordAuthenticatorManager passwordAuthenticatorManager)
    {
        requireNonNull(config, "config is null");

        installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
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

        pluginsLoaded.set(true);
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        PluginClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type %s", Plugin.class.getName());
        }

        for (Plugin plugin : plugins) {
            log.info("Installing %s", plugin.getClass().getName());
            installPlugin(plugin, pluginClassLoader::duplicate);
        }
    }

    public void installPlugin(Plugin plugin, Supplier<ClassLoader> duplicatePluginClassLoaderFactory)
    {
        for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
            log.info("Registering password authenticator %s", authenticatorFactory.getName());
            passwordAuthenticatorManager.addPasswordAuthenticatorFactory(authenticatorFactory);
        }
    }

    private PluginClassLoader buildClassLoader(String plugin)
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

    private PluginClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        PluginClassLoader classLoader = createClassLoader(artifacts, pomFile.getPath());

        Artifact artifact = artifacts.get(0);
        Set<String> plugins = discoverPlugins(artifact, classLoader, SERVICES_FILE, Plugin.class.getName());
        if (!plugins.isEmpty()) {
            writePluginServices(plugins, artifact.getFile(), SERVICES_FILE);
        }

        return classLoader;
    }

    private PluginClassLoader buildClassLoaderFromDirectory(File dir)
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

    private PluginClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }

    private PluginClassLoader createClassLoader(List<Artifact> artifacts, String name)
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

    private PluginClassLoader createClassLoader(List<URL> urls)
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
