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

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.TestHiveUtil.nonDefaultTimeZone;

public class TestHiveClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveClientConfig.class)
                .setTimeZone(TimeZone.getDefault().getID())
                .setMaxSplitSize(new DataSize(64, Unit.MEGABYTE))
                .setMaxOutstandingSplits(1_000)
                .setMaxSplitIteratorThreads(1_000)
                .setAllowDropTable(false)
                .setAllowRenameTable(false)
                .setAllowCorruptWritesForTesting(false)
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(1, TimeUnit.SECONDS))
                .setMaxMetastoreRefreshThreads(100)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(new DataSize(32, Unit.MEGABYTE))
                .setForceLocalScheduling(false)
                .setRecursiveDirWalkerEnabled(false)
                .setDfsTimeout(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setVerifyChecksum(true)
                .setResourceConfigFiles((String) null)
                .setHiveStorageFormat(HiveStorageFormat.RCBINARY)
                .setDomainSocketPath(null)
                .setUseParquetColumnNames(false)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3UseInstanceCredentials(true)
                .setS3SslEnabled(true)
                .setS3MaxClientRetries(3)
                .setS3MaxErrorRetries(10)
                .setS3MaxBackoffTime(new Duration(10, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(10, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(5, TimeUnit.SECONDS))
                .setS3MultipartMinFileSize(new DataSize(16, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(new DataSize(5, Unit.MEGABYTE))
                .setS3MaxConnections(500)
                .setS3StagingDirectory(new File(StandardSystemProperty.JAVA_IO_TMPDIR.value()))
                .setOptimizedReaderEnabled(true)
                .setAssumeCanonicalPartitionKeys(false)
                .setOrcMaxMergeDistance(new DataSize(1, Unit.MEGABYTE))
                .setOrcMaxBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.time-zone", nonDefaultTimeZone().getID())
                .put("hive.max-split-size", "256MB")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-split-iterator-threads", "10")
                .put("hive.allow-drop-table", "true")
                .put("hive.allow-rename-table", "true")
                .put("hive.allow-corrupt-writes-for-testing", "true")
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
                .put("hive.dfs.verify-checksum", "false")
                .put("hive.config.resources", "/foo.xml,/bar.xml")
                .put("hive.max-initial-splits", "10")
                .put("hive.max-initial-split-size", "16MB")
                .put("hive.recursive-directories", "true")
                .put("hive.storage-format", "SEQUENCEFILE")
                .put("hive.force-local-scheduling", "true")
                .put("hive.assume-canonical-partition-keys", "true")
                .put("dfs.domain-socket-path", "/foo")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.s3.aws-access-key", "abc123")
                .put("hive.s3.aws-secret-key", "secret")
                .put("hive.s3.use-instance-credentials", "false")
                .put("hive.s3.ssl.enabled", "false")
                .put("hive.s3.max-client-retries", "9")
                .put("hive.s3.max-error-retries", "8")
                .put("hive.s3.max-backoff-time", "4m")
                .put("hive.s3.max-retry-time", "20m")
                .put("hive.s3.connect-timeout", "8s")
                .put("hive.s3.socket-timeout", "4m")
                .put("hive.s3.multipart.min-file-size", "32MB")
                .put("hive.s3.multipart.min-part-size", "15MB")
                .put("hive.s3.max-connections", "77")
                .put("hive.s3.staging-directory", "/s3-staging")
                .put("hive.optimized-reader.enabled", "false")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setTimeZone(nonDefaultTimeZone().toTimeZone())
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingSplits(10)
                .setMaxSplitIteratorThreads(10)
                .setAllowDropTable(true)
                .setAllowRenameTable(true)
                .setAllowCorruptWritesForTesting(true)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMaxMetastoreRefreshThreads(2500)
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setMaxInitialSplits(10)
                .setMaxInitialSplitSize(new DataSize(16, Unit.MEGABYTE))
                .setForceLocalScheduling(true)
                .setRecursiveDirWalkerEnabled(true)
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setVerifyChecksum(false)
                .setResourceConfigFiles(ImmutableList.of("/foo.xml", "/bar.xml"))
                .setHiveStorageFormat(HiveStorageFormat.SEQUENCEFILE)
                .setDomainSocketPath("/foo")
                .setUseParquetColumnNames(true)
                .setS3AwsAccessKey("abc123")
                .setS3AwsSecretKey("secret")
                .setS3UseInstanceCredentials(false)
                .setS3SslEnabled(false)
                .setS3MaxClientRetries(9)
                .setS3MaxErrorRetries(8)
                .setS3MaxBackoffTime(new Duration(4, TimeUnit.MINUTES))
                .setS3MaxRetryTime(new Duration(20, TimeUnit.MINUTES))
                .setS3ConnectTimeout(new Duration(8, TimeUnit.SECONDS))
                .setS3SocketTimeout(new Duration(4, TimeUnit.MINUTES))
                .setS3MultipartMinFileSize(new DataSize(32, Unit.MEGABYTE))
                .setS3MultipartMinPartSize(new DataSize(15, Unit.MEGABYTE))
                .setS3MaxConnections(77)
                .setS3StagingDirectory(new File("/s3-staging"))
                .setOptimizedReaderEnabled(false)
                .setAssumeCanonicalPartitionKeys(true)
                .setOrcMaxMergeDistance(new DataSize(22, Unit.KILOBYTE))
                .setOrcMaxBufferSize(new DataSize(44, Unit.KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(55, Unit.KILOBYTE));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
