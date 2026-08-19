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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class TestLanceNamespaceHolder
{
    @Test
    public void testStoragePropertiesArePassedThrough()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            // lance.storage.* options are forwarded to the namespace with both the "lance." and
            // "storage." prefixes stripped, so that new options need no connector code change.
            Map<String, String> props = ImmutableMap.of(
                    "lance.root", tempDir.toString(),
                    "lance.storage.region", "us-east-1",
                    "lance.storage.aws_access_key_id", "test-key");
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                // No such table, so this falls back to the catalog-level storage options.
                Map<String, String> storageOptions =
                        holder.getStorageOptionsForTable(Collections.singletonList("no_such_table"));
                assertEquals(storageOptions.get("region"), "us-east-1");
                assertEquals(storageOptions.get("aws_access_key_id"), "test-key");
                assertEquals(storageOptions.size(), 2);
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
    public void testSchemaExistsIsFalseForMissingSchema()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(false);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                // A namespace that is genuinely absent must report false rather than throw.
                assertFalse(holder.schemaExists("no_such_schema"));
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
    public void testGetTablePathIsNullForMissingTable()
            throws Exception
    {
        Path tempDir = Files.createTempDirectory("lance-ns-test");
        try {
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> props = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder holder = new LanceNamespaceHolder(config, props);
            try {
                assertNull(holder.getTablePath("default", "no_such_table"));
                assertFalse(holder.tableExists("default", "no_such_table"));
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

    @Test
    public void testListTablesFollowsPageTokens()
    {
        LanceConfig config = new LanceConfig()
                .setImpl(PagingNamespace.class.getName())
                .setSingleLevelNs(false);
        LanceNamespaceHolder holder = new LanceNamespaceHolder(config, ImmutableMap.of());
        try {
            // The namespace serves three tables across two pages; a single unpaged call
            // would silently return only the first page.
            assertEquals(holder.listTables("my_schema"), ImmutableList.of("t1", "t2", "t3"));
        }
        finally {
            holder.shutdown();
        }
    }

    @Test
    public void testListSchemaNamesFollowsPageTokens()
    {
        LanceConfig config = new LanceConfig()
                .setImpl(PagingNamespace.class.getName())
                .setSingleLevelNs(false);
        LanceNamespaceHolder holder = new LanceNamespaceHolder(config, ImmutableMap.of());
        try {
            assertEquals(holder.listSchemaNames(), ImmutableList.of("ns1", "ns2", "ns3"));
        }
        finally {
            holder.shutdown();
        }
    }

    @Test(timeOut = 30_000)
    public void testRepeatedPageTokenTerminates()
    {
        LanceConfig config = new LanceConfig()
                .setImpl(StuckTokenNamespace.class.getName())
                .setSingleLevelNs(false);
        LanceNamespaceHolder holder = new LanceNamespaceHolder(config, ImmutableMap.of());
        try {
            // A server that keeps echoing the same token must not spin forever.
            assertEquals(holder.listTables("my_schema"), ImmutableList.of("t1"));
        }
        finally {
            holder.shutdown();
        }
    }

    /**
     * Serves tables and namespaces across two pages, keyed off the incoming page token.
     */
    public static class PagingNamespace
            implements LanceNamespace
    {
        @Override
        public void initialize(Map<String, String> properties, BufferAllocator allocator) {}

        @Override
        public String namespaceId()
        {
            return "paging";
        }

        @Override
        public ListTablesResponse listTables(ListTablesRequest request)
        {
            ListTablesResponse response = new ListTablesResponse();
            if (request.getPageToken() == null) {
                response.setTables(ImmutableSet.of("t1", "t2"));
                response.setPageToken("page2");
            }
            else {
                response.setTables(ImmutableSet.of("t3"));
            }
            return response;
        }

        @Override
        public ListNamespacesResponse listNamespaces(ListNamespacesRequest request)
        {
            ListNamespacesResponse response = new ListNamespacesResponse();
            if (request.getPageToken() == null) {
                response.setNamespaces(ImmutableSet.of("ns1", "ns2"));
                response.setPageToken("page2");
            }
            else {
                response.setNamespaces(ImmutableSet.of("ns3"));
            }
            return response;
        }
    }

    /**
     * Always returns the same page token, as a misbehaving server would.
     */
    public static class StuckTokenNamespace
            implements LanceNamespace
    {
        @Override
        public void initialize(Map<String, String> properties, BufferAllocator allocator) {}

        @Override
        public String namespaceId()
        {
            return "stuck";
        }

        @Override
        public ListTablesResponse listTables(ListTablesRequest request)
        {
            ListTablesResponse response = new ListTablesResponse();
            response.setTables(ImmutableSet.of("t1"));
            response.setPageToken("always-the-same");
            return response;
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
