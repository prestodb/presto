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
package com.facebook.presto.raptor;

import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestRaptorPlugin
{
    @Test
    public void testPlugin()
            throws Exception
    {
        RaptorPlugin plugin = loadPlugin(RaptorPlugin.class);

        plugin.setNodeManager(new InMemoryNodeManager());

        TypeRegistry typeRegistry = new TypeRegistry();
        plugin.setTypeManager(typeRegistry);

        plugin.setPageSorter(new PagesIndexPageSorter());

        List<ConnectorFactory> factories = plugin.getServices(ConnectorFactory.class);
        ConnectorFactory factory = getOnlyElement(factories);
        assertInstanceOf(factory, RaptorConnectorFactory.class);

        File tmpDir = Files.createTempDir();
        try {
            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .put("metadata.db.type", "h2")
                    .put("metadata.db.filename", tmpDir.getAbsolutePath())
                    .put("storage.data-directory", tmpDir.getAbsolutePath())
                    .build();

            factory.create("test", config);
        }
        finally {
            FileUtils.deleteRecursively(tmpDir);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}
