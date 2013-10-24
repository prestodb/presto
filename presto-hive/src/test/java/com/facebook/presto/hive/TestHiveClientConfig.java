/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestHiveClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveClientConfig.class)
                .setMaxSplitSize(new DataSize(64, Unit.MEGABYTE))
                .setMaxOutstandingSplits(10_000)
                .setMaxSplitIteratorThreads(50)
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(2, TimeUnit.MINUTES))
                .setMaxMetastoreRefreshThreads(100)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setDfsTimeout(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setFileSystemCacheTtl(new Duration(1, TimeUnit.DAYS))
                .setResourceConfigFiles((String) null)
                .setDomainSocketPath(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-split-size", "256MB")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-split-iterator-threads", "2")
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore-refresh-interval", "30m")
                .put("hive.metastore-refresh-max-threads", "2500")
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.metastore-timeout", "20s")
                .put("hive.metastore.partition-batch-size.min", "1")
                .put("hive.metastore.partition-batch-size.max", "1000")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.file-system-cache-ttl", "2d")
                .put("hive.config.resources", "/foo.xml,/bar.xml")
                .put("dfs.domain-socket-path", "/foo")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingSplits(10)
                .setMaxSplitIteratorThreads(2)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMaxMetastoreRefreshThreads(2500)
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setFileSystemCacheTtl(new Duration(2, TimeUnit.DAYS))
                .setResourceConfigFiles(ImmutableList.of("/foo.xml", "/bar.xml"))
                .setDomainSocketPath("/foo");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
