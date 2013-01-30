package com.facebook.presto.hive;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "hive")
public class TestHiveClientChunkIteratorBackPressure
    extends AbstractTestHiveClient
{
    @Parameters({"hiveMetastoreHost", "hiveMetastorePort"})
    @BeforeMethod
    public void setup(String host, int port)
            throws Exception
    {
        // Restrict the outstanding chunks to 1 and only use 2 threads per iterator
        this.client = new CachingHiveClient(new DummyMetadataCache(), new HiveClient(host, port, 1024 * 1024 * 1024 /* 1 GB */, 1, 2, getHivePartitionChunkCodec()));
    }
}
