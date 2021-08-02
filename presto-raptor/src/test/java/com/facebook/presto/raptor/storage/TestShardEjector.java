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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.filesystem.LocalFileStorageService;
import com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestShardEjector
{
    private IDBI dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private File dataDir;
    private StorageService storageService;

    @BeforeMethod
    public void setup()
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        shardManager = createShardManager(dbi);

        dataDir = createTempDir();
        storageService = new LocalFileStorageService(new LocalOrcDataEnvironment(), dataDir.toURI());
        storageService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test(invocationCount = 20)
    public void testEjector()
            throws Exception
    {
        NodeManager nodeManager = createNodeManager("node1", "node2", "node3", "node4", "node5");

        ShardEjector ejector = new ShardEjector(
                nodeManager.getCurrentNode().getNodeIdentifier(),
                nodeManager::getWorkerNodes,
                shardManager,
                storageService,
                new Duration(1, HOURS),
                Optional.of(new TestingBackupStore()),
                new LocalOrcDataEnvironment(),
                "test");

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo("node1", 14))
                .add(shardInfo("node1", 13))
                .add(shardInfo("node1", 12))
                .add(shardInfo("node1", 11))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node1", 10))
                .add(shardInfo("node2", 5))
                .add(shardInfo("node2", 5))
                .add(shardInfo("node3", 10))
                .add(shardInfo("node4", 10))
                .add(shardInfo("node5", 10))
                .add(shardInfo("node6", 200))
                .build();

        long tableId = createTable("test");
        List<ColumnInfo> columns = ImmutableList.of(new ColumnInfo(1, BIGINT));

        shardManager.createTable(tableId, columns, false, OptionalLong.empty(), false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty(), 0);

        for (ShardInfo shard : shards.subList(0, 8)) {
            File file = new File(storageService.getStorageFile(shard.getShardUuid()).toString());
            storageService.createParents(new Path(file.toURI()));
            assertTrue(file.createNewFile());
        }

        ejector.process();

        shardManager.getShardNodes(tableId, TupleDomain.all(), false);

        Set<UUID> ejectedShards = shards.subList(0, 4).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());
        Set<UUID> keptShards = shards.subList(4, 8).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());

        Set<UUID> remaining = uuids(shardManager.getNodeShardsAndDeltas("node1"));

        for (UUID uuid : ejectedShards) {
            assertFalse(remaining.contains(uuid));
            assertFalse(new File(storageService.getStorageFile(uuid).toString()).exists());
        }

        assertEquals(remaining, keptShards);
        for (UUID uuid : keptShards) {
            assertTrue(new File(storageService.getStorageFile(uuid).toString()).exists());
        }

        Set<UUID> others = ImmutableSet.<UUID>builder()
                .addAll(uuids(shardManager.getNodeShardsAndDeltas("node2")))
                .addAll(uuids(shardManager.getNodeShardsAndDeltas("node3")))
                .addAll(uuids(shardManager.getNodeShardsAndDeltas("node4")))
                .addAll(uuids(shardManager.getNodeShardsAndDeltas("node5")))
                .build();

        assertTrue(others.containsAll(ejectedShards));
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, false, null, 0, false);
    }

    private static Set<UUID> uuids(Set<ShardMetadata> metadata)
    {
        return metadata.stream()
                .map(ShardMetadata::getShardUuid)
                .collect(toSet());
    }

    private static ShardInfo shardInfo(String node, long size)
    {
        return new ShardInfo(randomUUID(), OptionalInt.empty(), ImmutableSet.of(node), ImmutableList.of(), 1, size, size * 2, 0);
    }

    private static NodeManager createNodeManager(String current, String... others)
    {
        Node currentNode = createTestingNode(current);
        TestingNodeManager nodeManager = new TestingNodeManager(currentNode);
        for (String other : others) {
            nodeManager.addNode(createTestingNode(other));
        }
        return nodeManager;
    }

    private static Node createTestingNode(String identifier)
    {
        return new InternalNode(identifier, URI.create("http://test"), NodeVersion.UNKNOWN, false);
    }

    private static class TestingBackupStore
            implements BackupStore
    {
        @Override
        public void backupShard(UUID uuid, File source)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void restoreShard(UUID uuid, File target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteShard(UUID uuid)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean shardExists(UUID uuid)
        {
            return true;
        }
    }
}
