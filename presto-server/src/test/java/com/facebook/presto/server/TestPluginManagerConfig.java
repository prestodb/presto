/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.tesla.aether.TeslaAether;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestPluginManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PluginManagerConfig.class)
                .setInstalledPluginsDir(new File("plugin"))
                .setPlugins((String)null)
                .setPluginConfigurationDir(new File("etc/"))
                .setMavenLocalRepository(TeslaAether.DEFAULT_LOCAL_REPOSITORY.getAbsolutePath())
                .setMavenRemoteRepository(TeslaAether.DEFAULT_REMOTE_REPOSITORY));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("plugin.dir", "plugins-dir")
                .put("plugin.bundles", "a,b,c")
                .put("plugin.config-dir", "plugin-configs")
                .put("maven.repo.local", "local-repo")
                .put("maven.repo.remote", "remote-a,remote-b")
                .build();

        PluginManagerConfig expected = new PluginManagerConfig()
                .setInstalledPluginsDir(new File("plugins-dir"))
                .setPlugins(ImmutableList.of("a", "b", "c"))
                .setPluginConfigurationDir(new File("plugin-configs"))
                .setMavenLocalRepository("local-repo")
                .setMavenRemoteRepository(ImmutableList.of("remote-a", "remote-b"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
