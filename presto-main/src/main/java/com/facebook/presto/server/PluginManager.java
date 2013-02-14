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
    private final List<String> plugins;
    private final File pluginConfigurationDir;
    private final Map<String, String> optionalConfig;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(NodeInfo nodeInfo, PluginManagerConfig config, ImportClientManager importClientManager, ConfigurationFactory configurationFactory)
    {
        this.importClientManager = importClientManager;
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
        // todo support plugin as single jar file and plugin as a directory of jar files
        File pomFile = new File(plugin);
        List<Artifact> artifacts = resolver.resolvePom(pomFile);

        log.debug("Classpath for %s:", pomFile);
        List<URL> urls = new ArrayList<>();
        urls.add(new File(pomFile.getParentFile(), "target/classes/").toURL());
        for (Artifact artifact : artifacts) {
            if (artifact.getFile() != null) {
                log.debug("    %s", artifact.getFile());
                urls.add(artifact.getFile().toURL());
            }
            else {
                log.debug("  Could not resolve artifact %s", artifact);
            }
        }

        // todo add basic child first class loader
        return new URLClassLoader(urls.toArray(new URL[urls.size()]));
    }
}
