package com.facebook.presto.hive;

import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = "hive")
public class TestCachingHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hiveMetastoreHost", "hiveMetastorePort"})
    @BeforeMethod
    public void setup(String host, int port)
            throws Exception
    {
        this.client = new CachingHiveClient(new HiveMetadataCache(new Duration(60.0, TimeUnit.MINUTES)), new HiveClient(host, port, 1024 * 1024 * 1024 /* 1 GB */, 100, 50, getHivePartitionChunkCodec()));
    }
}
