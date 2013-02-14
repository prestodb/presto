/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.resolver.ArtifactResolver;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;

// todo add test
public class PluginManagerConfig
{
    private List<String> plugins;
    private File pluginConfigurationDir = new File("etc/");
    private String mavenLocalRepository = ArtifactResolver.USER_LOCAL_REPO;
    private List<String> mavenRemoteRepository = ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI);

    public List<String> getPlugins()
    {
        return plugins;
    }

    public PluginManagerConfig setPlugins(List<String> plugins)
    {
        this.plugins = plugins;
        return this;
    }

    @Config("plugin.bundles")
    public PluginManagerConfig setPlugins(String plugins)
    {
        this.plugins = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(plugins));
        return this;
    }

    @NotNull
    public File getPluginConfigurationDir()
    {
        return pluginConfigurationDir;
    }

    @Config("plugin.config-dir")
    public PluginManagerConfig setPluginConfigurationDir(File pluginConfigurationDir)
    {
        this.pluginConfigurationDir = pluginConfigurationDir;
        return this;
    }

    @NotNull
    public String getMavenLocalRepository()
    {
        return mavenLocalRepository;
    }

    @Config("maven.repo.local")
    public PluginManagerConfig setMavenLocalRepository(String mavenLocalRepository)
    {
        this.mavenLocalRepository = mavenLocalRepository;
        return this;
    }

    @NotNull
    public List<String> getMavenRemoteRepository()
    {
        return mavenRemoteRepository;
    }

    public PluginManagerConfig setMavenRemoteRepository(List<String> mavenRemoteRepository)
    {
        this.mavenRemoteRepository = mavenRemoteRepository;
        return this;
    }

    @Config("maven.repo.remote")
    public PluginManagerConfig setMavenRemoteRepository(String mavenRemoteRepository)
    {
        this.mavenRemoteRepository = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(mavenRemoteRepository));
        return this;
    }
}
