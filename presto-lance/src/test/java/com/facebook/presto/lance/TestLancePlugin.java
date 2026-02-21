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
package com.facebook.presto.lance;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestLancePlugin
{
    @Test
    public void testCreateConnector()
            throws Exception
    {
        ConnectorFactory factory = StreamSupport
                .stream(new LancePlugin().getConnectorFactories().spliterator(), false)
                .collect(MoreCollectors.onlyElement());
        assertNotNull(factory);
        assertEquals(factory.getName(), "lance");
        Path tempDir = Files.createTempDirectory("lance-test");
        try {
            factory.create(
                    "test",
                    ImmutableMap.of("lance.root-url", tempDir.toString()),
                    new TestingConnectorContext())
                    .shutdown();
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    private static void deleteRecursively(Path path)
            throws Exception
    {
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                for (Path entry : (Iterable<Path>) entries::iterator) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
