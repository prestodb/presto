package com.facebook.presto.hive;

import io.airlift.json.JsonCodecFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveClientFactory
{
    @Test
    public void testClusterSelection()
            throws Exception
    {
        HiveClientFactory hiveClientFactory = new HiveClientFactory(
                new HiveClientConfig(),
                new HiveChunkEncoder(new JsonCodecFactory().jsonCodec(HivePartitionChunk.class)),
                new HdfsEnvironment());

        // Two distinct Hive clusters
        final MockHiveMetastoreClient mockClient1 = new MockHiveMetastoreClient();
        HiveCluster hiveCluster1 = new HiveCluster()
        {
            @Override
            public HiveMetastoreClient createMetastoreClient()
            {
                return mockClient1;
            }
        };
        final MockHiveMetastoreClient mockClient2 = new MockHiveMetastoreClient();
        HiveCluster hiveCluster2 = new HiveCluster()
        {
            @Override
            public HiveMetastoreClient createMetastoreClient()
            {
                return mockClient2;
            }
        };

        // Creating multiple clients for the same cluster should hit the same cache
        Assert.assertEquals(mockClient1.getAccessCount(), 0);
        hiveClientFactory.get(hiveCluster1).listSchemaNames();
        Assert.assertEquals(mockClient1.getAccessCount(), 1);
        hiveClientFactory.get(hiveCluster1).listSchemaNames();
        Assert.assertEquals(mockClient1.getAccessCount(), 1);

        // Another hive cluster should have its own cache
        Assert.assertEquals(mockClient2.getAccessCount(), 0);
        hiveClientFactory.get(hiveCluster2).listSchemaNames();
        Assert.assertEquals(mockClient2.getAccessCount(), 1);
        hiveClientFactory.get(hiveCluster2).listSchemaNames();
        Assert.assertEquals(mockClient2.getAccessCount(), 1);
    }
}
