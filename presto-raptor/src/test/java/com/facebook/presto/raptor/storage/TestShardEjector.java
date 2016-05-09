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

import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorNodeSupplier;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
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
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestShardEjector
{
    private IDBI dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private File dataDir;
    private StorageService storageService;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        shardManager = createShardManager(dbi);

        dataDir = createTempDir();
        storageService = new FileStorageService(dataDir);
        storageService.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
    }

    @Test(invocationCount = 20)
    public void testEjector()
            throws Exception
    {
        NodeManager nodeManager = createNodeManager("node1", "node2", "node3", "node4", "node5");

        ShardEjector ejector = new ShardEjector(
                nodeManager.getCurrentNode().getNodeIdentifier(),
                new RaptorNodeSupplier(nodeManager, new RaptorConnectorId("test")),
                shardManager,
                storageService,
                new Duration(1, HOURS),
                Optional.of(new TestingBackupStore()),
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

        shardManager.createTable(tableId, columns, false);

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableId, columns, shards, Optional.empty());

        for (ShardInfo shard : shards.subList(0, 8)) {
            File file = storageService.getStorageFile(shard.getShardUuid());
            storageService.createParents(file);
            assertTrue(file.createNewFile());
        }

        ejector.process();

        shardManager.getShardNodes(tableId, false, false, TupleDomain.all());

        Set<UUID> ejectedShards = shards.subList(0, 4).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());
        Set<UUID> keptShards = shards.subList(4, 8).stream()
                .map(ShardInfo::getShardUuid)
                .collect(toSet());

        Set<UUID> remaining = uuids(shardManager.getNodeShards("node1"));

        for (UUID uuid : ejectedShards) {
            assertFalse(remaining.contains(uuid));
            assertFalse(storageService.getStorageFile(uuid).exists());
        }

        assertEquals(remaining, keptShards);
        for (UUID uuid : keptShards) {
            assertTrue(storageService.getStorageFile(uuid).exists());
        }

        Set<UUID> others = ImmutableSet.<UUID>builder()
                .addAll(uuids(shardManager.getNodeShards("node2")))
                .addAll(uuids(shardManager.getNodeShards("node3")))
                .addAll(uuids(shardManager.getNodeShards("node4")))
                .addAll(uuids(shardManager.getNodeShards("node5")))
                .build();

        assertTrue(others.containsAll(ejectedShards));
    }

    private long createTable(String name)
    {
        return dbi.onDemand(MetadataDao.class).insertTable("test", name, false, null);
    }

    private static Set<UUID> uuids(Set<ShardMetadata> metadata)
    {
        return metadata.stream()
                .map(ShardMetadata::getShardUuid)
                .collect(toSet());
    }

    private static ShardInfo shardInfo(String node, long size)
    {
        return new ShardInfo(randomUUID(), OptionalInt.empty(), ImmutableSet.of(node), ImmutableList.of(), 1, size, size * 2);
    }

    private static NodeManager createNodeManager(String current, String... others)
    {
        Node currentNode = new TestingNode(current);

        ImmutableSet.Builder<Node> nodes = ImmutableSet.builder();
        nodes.add(currentNode);
        for (String other : others) {
            nodes.add(new TestingNode(other));
        }

        return new TestingNodeManager(nodes.build(), currentNode);
    }

    private static class TestingNodeManager
            implements NodeManager
    {
        private final Set<Node> nodes;
        private final Node currentNode;

        public TestingNodeManager(Set<Node> nodes, Node currentNode)
        {
            this.nodes = ImmutableSet.copyOf(requireNonNull(nodes, "nodes is null"));
            this.currentNode = requireNonNull(currentNode, "currentNode is null");
        }

        @Override
        public Set<Node> getNodes(NodeState state)
        {
            return nodes;
        }

        @Override
        public Set<Node> getActiveDatasourceNodes(String datasourceName)
        {
            return nodes;
        }

        @Override
        public Node getCurrentNode()
        {
            return currentNode;
        }

        @Override
        public Set<Node> getCoordinators()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingNode
            implements Node
    {
        private final String identifier;

        public TestingNode(String identifier)
        {
            this.identifier = requireNonNull(identifier, "identifier is null");
        }

        @Override
        public HostAddress getHostAndPort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getHttpUri()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getNodeIdentifier()
        {
            return identifier;
        }
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
