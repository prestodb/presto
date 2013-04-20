package com.facebook.presto.hive;

import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

public class TestHiveClientSplitIteratorBackPressure
        extends AbstractTestHiveClient
{
    @Parameters({"hiveMetastoreHost", "hiveMetastorePort"})
    @BeforeMethod
    public void setup(String host, int port)
            throws Exception
    {
        // Restrict the outstanding splits to 1 and only use 2 threads per iterator
        HiveClient client = new HiveClient(
                new HiveConnectorId("hive"),
                new CachingHiveMetastore(new TestingHiveCluster(host, port), Duration.valueOf("1m")),
                new HdfsEnvironment(),
                MoreExecutors.sameThreadExecutor(),
                1,
                2,
                500);

        metadata = client;
        splitManager = client;
        recordSetProvider = client;
    }
}
