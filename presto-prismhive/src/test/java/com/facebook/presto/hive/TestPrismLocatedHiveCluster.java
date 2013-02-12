package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPrismLocatedHiveCluster
{
    @Test
    public void testPrismLocatedHiveCluster()
            throws Exception
    {
        HiveMetastoreClientFactory factory = new HiveMetastoreClientFactory(new HiveClientConfig());
        ImmutableList<HostAndPort> localhost = ImmutableList.of(HostAndPort.fromParts("localhost", 1111));
        PrismHiveClient.PrismLocatedHiveCluster cluster1a = new PrismHiveClient.PrismLocatedHiveCluster("metastore1", localhost, factory);
        PrismHiveClient.PrismLocatedHiveCluster cluster1b = new PrismHiveClient.PrismLocatedHiveCluster("metastore1", localhost, factory);
        PrismHiveClient.PrismLocatedHiveCluster cluster2 = new PrismHiveClient.PrismLocatedHiveCluster("metastore2", localhost, factory);

        // Metastore name should be used to determine uniqueness
        Assert.assertEquals(cluster1a, cluster1b);
        Assert.assertNotEquals(cluster1a, cluster2);
    }
}
