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
package com.facebook.presto.hive.metastore;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMetastoreClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(MetastoreClientConfig.class)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setVerifyChecksum(true)
                .setRequireHadoopNative(true)
                .setMetastoreCacheTtl(new Duration(0, TimeUnit.SECONDS))
                .setMetastoreRefreshInterval(new Duration(0, TimeUnit.SECONDS))
                .setMetastoreCacheMaximumSize(10000)
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setMaxMetastoreRefreshThreads(100)
                .setRecordingPath(null)
                .setRecordingDuration(new Duration(0, TimeUnit.MINUTES))
                .setReplay(false)
                .setPartitionVersioningEnabled(false)
                .setMetastoreCacheScope(MetastoreCacheScope.ALL)
                .setMetastoreImpersonationEnabled(false)
                .setPartitionCacheValidationPercentage(0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.metastore-timeout", "20s")
                .put("hive.dfs.verify-checksum", "false")
                .put("hive.dfs.require-hadoop-native", "false")
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore-refresh-interval", "30m")
                .put("hive.metastore-cache-maximum-size", "5000")
                .put("hive.per-transaction-metastore-cache-maximum-size", "500")
                .put("hive.metastore-refresh-max-threads", "2500")
                .put("hive.metastore-recording-path", "/foo/bar")
                .put("hive.metastore-recoding-duration", "42s")
                .put("hive.replay-metastore-recording", "true")
                .put("hive.partition-versioning-enabled", "true")
                .put("hive.metastore-cache-scope", "PARTITION")
                .put("hive.metastore-impersonation-enabled", "true")
                .put("hive.partition-cache-validation-percentage", "60.0")
                .build();

        MetastoreClientConfig expected = new MetastoreClientConfig()
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setVerifyChecksum(false)
                .setRequireHadoopNative(false)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMetastoreCacheMaximumSize(5000)
                .setPerTransactionMetastoreCacheMaximumSize(500)
                .setMaxMetastoreRefreshThreads(2500)
                .setRecordingPath("/foo/bar")
                .setRecordingDuration(new Duration(42, TimeUnit.SECONDS))
                .setReplay(true)
                .setPartitionVersioningEnabled(true)
                .setMetastoreCacheScope(MetastoreCacheScope.PARTITION)
                .setMetastoreImpersonationEnabled(true)
                .setPartitionCacheValidationPercentage(60.0);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
