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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.resolver.ArtifactResolver;
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
                .setPlugins((String) null)
                .setMavenLocalRepository(ArtifactResolver.USER_LOCAL_REPO)
                .setMavenRemoteRepository(ArtifactResolver.MAVEN_CENTRAL_URI));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("plugin.dir", "plugins-dir")
                .put("plugin.bundles", "a,b,c")
                .put("maven.repo.local", "local-repo")
                .put("maven.repo.remote", "remote-a,remote-b")
                .build();

        PluginManagerConfig expected = new PluginManagerConfig()
                .setInstalledPluginsDir(new File("plugins-dir"))
                .setPlugins(ImmutableList.of("a", "b", "c"))
                .setMavenLocalRepository("local-repo")
                .setMavenRemoteRepository(ImmutableList.of("remote-a", "remote-b"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
