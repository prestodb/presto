package com.facebook.presto.metadata;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestDatabaseShardManager
{
    private Handle dummyHandle;
    private ShardManager shardManager;

    @BeforeMethod
    public void setupDatabase()
            throws Exception
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();

        // temporary, will go away with unified DAO.
        MetadataDao.Utils.createMetadataTables(dbi.onDemand(MetadataDao.class));

        shardManager = new DatabaseShardManager(dbi);
    }

    @AfterMethod
    public void cleanupDatabase()
    {
        dummyHandle.close();
    }

    @Test
    public void testImportFlow()
    {
        long tableId = 123;

        // verify creating multiple times is idempotent
        for (int i = 0; i < 5; i++) {
            shardManager.createImportTable(tableId, "hive", "default", "orders");
        }

        List<SerializedPartitionChunk> partitionChunks = createRandomChunks(5);
        List<Long> shardIds = shardManager.createImportPartition(tableId, "ds=2012-10-15", partitionChunks);

        ImmutableList<String> nodes = ImmutableList.of("foo", "bar", "baz");
        int nodeIndex = 0;
        for (long shardId : shardIds) {
            String node = nodes.get(nodeIndex++ % nodes.size());
            shardManager.commitShard(shardId, node);
        }

        Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodes(tableId);
        assertEquals(shardNodes, ImmutableMultimap.of(
                1L, "foo", 2L, "bar", 3L, "baz", 4L, "foo", 5L, "bar"));
    }

    public List<SerializedPartitionChunk> createRandomChunks(int n)
    {
        ImmutableList.Builder<SerializedPartitionChunk> list = ImmutableList.builder();
        for (int i = 0; i < n; i++) {
            list.add(new SerializedPartitionChunk(randomBytes()));
        }
        return list.build();
    }

    public byte[] randomBytes()
    {
        Random random = new Random();
        int n = 50 + random.nextInt(50);
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        for (int i = 0; i < n; i++) {
            //noinspection CharUsedInArithmeticContext
            out.write('a' + random.nextInt(26));
        }
        return out.toByteArray();
    }
}
