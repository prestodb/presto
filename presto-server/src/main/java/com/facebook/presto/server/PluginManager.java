/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.ImportClientFactoryFactory;
import com.facebook.presto.split.ImportClientManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.fromProperties;

public class PluginManager
{
    private static final Logger log = Logger.get(PluginManager.class);

    private final ImportClientManager importClientManager;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final File pluginConfigurationDir;
    private final Map<String, String> optionalConfig;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(NodeInfo nodeInfo, PluginManagerConfig config, ImportClientManager importClientManager, ConfigurationFactory configurationFactory)
    {
        this.importClientManager = importClientManager;
        installedPluginsDir = config.getInstalledPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.pluginConfigurationDir = config.getPluginConfigurationDir();
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        TreeMap<String, String> optionalConfig = new TreeMap<>(configurationFactory.getProperties());
        optionalConfig.put("node.id", nodeInfo.getNodeId());
        this.optionalConfig = ImmutableMap.copyOf(optionalConfig);
    }

    public boolean arePluginsLoaded()
    {
        return pluginsLoaded.get();
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoaded.compareAndSet(false, true)) {
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
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader);

            ServiceLoader<ImportClientFactoryFactory> serviceLoader = ServiceLoader.load(ImportClientFactoryFactory.class, pluginClassLoader);
            List<ImportClientFactoryFactory> importClientFactoryFactories = ImmutableList.copyOf(serviceLoader);

            for (ImportClientFactoryFactory importClientFactoryFactory : importClientFactoryFactories) {
                Map<String, String> requiredConfig = loadPluginConfig(importClientFactoryFactory.getConfigName());
                ImportClientFactory importClientFactory = importClientFactoryFactory.createImportClientFactory(requiredConfig, optionalConfig);
                importClientManager.addImportClientFactory(importClientFactory);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private Map<String, String> loadPluginConfig(String name)
            throws Exception
    {
        Preconditions.checkNotNull(name, "name is null");

        Properties properties = new Properties();
        if (pluginConfigurationDir != null) {
            File configFile = new File(pluginConfigurationDir, name + ".properties");
            if (configFile.canRead()) {
                try (FileInputStream in = new FileInputStream(configFile)) {
                    properties.load(in);
                }
            }
        }
        return fromProperties(properties);
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws MalformedURLException
    {
        File file = new File(plugin);
        if (file.isFile() && file.getName().endsWith(".xml")) {
            return buildClassLoaderFromPom(file);
        }
        else if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        else {
            return buildClassLoaderFromCoordinates(plugin);
        }
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws MalformedURLException
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);

        log.debug("Classpath for %s:", pomFile);
        List<URL> urls = new ArrayList<>();
        urls.add(new File(pomFile.getParentFile(), "target/classes/").toURI().toURL());
        for (Artifact artifact : artifacts) {
            if (artifact.getFile() != null) {
                log.debug("    %s", artifact.getFile());
                urls.add(artifact.getFile().toURI().toURL());
            }
            else {
                log.debug("  Could not resolve artifact %s", artifact);
            }
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws MalformedURLException
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
            throws MalformedURLException
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);

        log.debug("Classpath for %s:", rootArtifact);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : artifacts) {
            if (artifact.getFile() != null) {
                log.debug("    %s", artifact.getFile());
                urls.add(artifact.getFile().toURI().toURL());
            }
            else {
                log.debug("  Could not resolve artifact %s", artifact);
            }
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        // todo add basic child first class loader
        return new URLClassLoader(urls.toArray(new URL[urls.size()]));
    }

    private List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
