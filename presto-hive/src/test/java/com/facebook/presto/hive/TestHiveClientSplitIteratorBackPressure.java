package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

public class TestHiveClientSplitIteratorBackPressure
        extends AbstractTestHiveClient
{
    private static final String CONNECTOR_ID = "hive-push-back-test";

    @Parameters({"hiveMetastoreHost", "hiveMetastorePort", "hiveDatabaseName"})
    @BeforeMethod
    public void setup(String host, int port, String databaseName)
            throws Exception
    {
        setupHive(CONNECTOR_ID, databaseName);

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        // Restrict the outstanding splits to 1 and only use 2 threads per iterator
        HiveClient client = new HiveClient(
                new HiveConnectorId(CONNECTOR_ID),
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
