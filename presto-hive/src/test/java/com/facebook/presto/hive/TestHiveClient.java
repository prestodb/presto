package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
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
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        FileSystemWrapper fileSystemWrapper = new FileSystemWrapperProvider(new FileSystemCache(hiveClientConfig),
                new SlowDatanodeSwitcher(hiveClientConfig),
                hiveClientConfig).get();

        HiveClient client = new HiveClient(
                new HiveConnectorId("hive"),
                new CachingHiveMetastore(new TestingHiveCluster(host, port), Duration.valueOf("1m")),
                new HdfsEnvironment(new HdfsConfiguration(), fileSystemWrapper),
                MoreExecutors.sameThreadExecutor(),
                100,
                50,
                500);

        metadata = client;
        splitManager = client;
        recordSetProvider = client;
    }
}
