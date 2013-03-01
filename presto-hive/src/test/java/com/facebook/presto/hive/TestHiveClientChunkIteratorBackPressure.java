package com.facebook.presto.hive;

import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

public class TestHiveClientChunkIteratorBackPressure
    extends AbstractTestHiveClient
{
    @Parameters({"hiveMetastoreHost", "hiveMetastorePort"})
    @BeforeMethod
    public void setup(String host, int port)
            throws Exception
    {
        // Restrict the outstanding chunks to 1 and only use 2 threads per iterator
        this.client = new HiveClient(
                1024 * 1024 * 1024 /* 1 GB */,
                1,
                2,
                500,
                getHiveChunkEncoder(),
                new CachingHiveMetastore(new SimpleHiveCluster(host, port), Duration.valueOf("1m")),
                new FileSystemCache(),
                MoreExecutors.sameThreadExecutor());
    }
}
