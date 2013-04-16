package com.facebook.presto.hive;

import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

public class TestHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hiveMetastoreHost", "hiveMetastorePort"})
    @BeforeMethod
    public void setup(String host, int port)
            throws Exception
    {
        this.client = new HiveClient(
                1024 * 1024 * 1024 /* 1 GB */,
                100,
                50,
                500,
                getHiveChunkEncoder(),
                new HiveChunkReader(new HdfsEnvironment()),
                new CachingHiveMetastore(new TestingHiveCluster(host, port), Duration.valueOf("1m")),
                new HdfsEnvironment(),
                MoreExecutors.sameThreadExecutor());
    }
}
