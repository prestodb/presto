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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestLanceNamespaceHolder
{
    @Test
    public void testGetTableIdSingleLevel()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                // In single-level mode, schema is omitted from tableId
                List<String> tableId = holder.getTableId("default", "my_table");
                assertEquals(tableId, Collections.singletonList("my_table"));
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testGetTableIdMultiLevel()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(false);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                // In multi-level mode, schema is included in tableId
                List<String> tableId = holder.getTableId("my_schema", "my_table");
                assertEquals(tableId.size(), 2);
                assertEquals(tableId.get(0), "my_schema");
                assertEquals(tableId.get(1), "my_table");
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testGetTableIdWithParent()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig()
                    .setSingleLevelNs(false)
                    .setParent("org$warehouse");
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                List<String> tableId = holder.getTableId("my_schema", "my_table");
                assertEquals(tableId.size(), 4);
                assertEquals(tableId.get(0), "org");
                assertEquals(tableId.get(1), "warehouse");
                assertEquals(tableId.get(2), "my_schema");
                assertEquals(tableId.get(3), "my_table");
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testPrestoSchemaToLanceNamespaceSingleLevel()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                // Single-level mode maps to empty namespace (root)
                List<String> ns = holder.prestoSchemaToLanceNamespace("default");
                assertEquals(ns, Collections.emptyList());
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testPrestoSchemaToLanceNamespaceMultiLevel()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(false);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                List<String> ns = holder.prestoSchemaToLanceNamespace("my_schema");
                assertEquals(ns, Collections.singletonList("my_schema"));
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testPrestoSchemaToLanceNamespaceWithParent()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig()
                    .setSingleLevelNs(false)
                    .setParent("p1$p2");
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                List<String> ns = holder.prestoSchemaToLanceNamespace("my_schema");
                assertEquals(ns.size(), 3);
                assertEquals(ns.get(0), "p1");
                assertEquals(ns.get(1), "p2");
                assertEquals(ns.get(2), "my_schema");
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    public void testSchemaExistsSingleLevel()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                assertEquals(holder.schemaExists("default"), true);
                assertEquals(holder.schemaExists("other"), false);
            }
            finally {
                holder.shutdown();
            }
        }
        finally {
            deleteRecursively(tempDir);
        }
    }

    private static void deleteRecursively(Path path)
            throws Exception
    {
        if (java.nio.file.Files.isDirectory(path)) {
            try (java.util.stream.Stream<Path> entries = java.nio.file.Files.list(path)) {
                for (Path entry : (Iterable<Path>) entries::iterator) {
                    deleteRecursively(entry);
                }
            }
        }
        java.nio.file.Files.deleteIfExists(path);
    }
}
