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
import com.google.common.collect.ImmutableSet;
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
                .setMaxPartitionsPerScan(100_000)
                .setMaxOutstandingSplits(1_000)
                .setMaxSplitIteratorThreads(1_000)
                .setAllowCorruptWritesForTesting(false)
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(1, TimeUnit.SECONDS))
                .setMetastoreCacheMaximumSize(10000)
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setMaxMetastoreRefreshThreads(100)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(new DataSize(32, Unit.MEGABYTE))
                .setRespectSplitsInputFormats((String) null)
                .setDomainCompactionThreshold(100)
                .setForceLocalScheduling(false)
                .setMaxConcurrentFileRenames(20)
                .setRecursiveDirWalkerEnabled(false)
                .setDfsTimeout(new Duration(60, TimeUnit.SECONDS))
                .setIpcPingInterval(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setVerifyChecksum(true)
                .setDomainSocketPath(null)
                .setResourceConfigFiles((String) null)
                .setHiveStorageFormat(HiveStorageFormat.RCBINARY)
                .setHiveCompressionCodec(HiveCompressionCodec.GZIP)
                .setRespectTableFormat(true)
                .setImmutablePartitions(false)
                .setMaxPartitionsPerWriter(100)
                .setUseParquetColumnNames(false)
                .setUseOrcColumnNames(false)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Endpoint(null)
                .setS3SignerType(null)
                .setS3UseInstanceCredentials(true)
                .setS3SslEnabled(true)
                .setS3SseEnabled(false)
                .setS3KmsKeyId(null)
                .setS3EncryptionMaterialsProvider(null)
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
                .setPinS3ClientToCurrentRegion(false)
                .setS3UserAgentPrefix("")
                .setParquetPredicatePushdownEnabled(false)
                .setParquetOptimizedReaderEnabled(false)
                .setAssumeCanonicalPartitionKeys(false)
                .setOrcBloomFiltersEnabled(false)
                .setOrcMaxMergeDistance(new DataSize(1, Unit.MEGABYTE))
                .setOrcMaxBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setRcfileOptimizedReaderEnabled(false)
                .setHiveMetastoreAuthenticationType(HiveClientConfig.HiveMetastoreAuthenticationType.NONE)
                .setHiveMetastoreServicePrincipal(null)
                .setHiveMetastoreClientPrincipal(null)
                .setHiveMetastoreClientKeytab(null)
                .setHdfsAuthenticationType(HiveClientConfig.HdfsAuthenticationType.NONE)
                .setHdfsImpersonationEnabled(false)
                .setHdfsPrestoPrincipal(null)
                .setHdfsPrestoKeytab(null)
                .setSkipDeletionForAlter(false)
                .setBucketExecutionEnabled(true)
                .setBucketWritingEnabled(true)
                .setFileSystemMaxCacheSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.time-zone", nonDefaultTimeZone().getID())
                .put("hive.max-split-size", "256MB")
                .put("hive.max-partitions-per-scan", "123")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-split-iterator-threads", "10")
                .put("hive.allow-corrupt-writes-for-testing", "true")
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore-refresh-interval", "30m")
                .put("hive.metastore-cache-maximum-size", "5000")
                .put("hive.per-transaction-metastore-cache-maximum-size", "500")
                .put("hive.metastore-refresh-max-threads", "2500")
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.metastore-timeout", "20s")
                .put("hive.metastore.partition-batch-size.min", "1")
                .put("hive.metastore.partition-batch-size.max", "1000")
                .put("hive.dfs.ipc-ping-interval", "34s")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.dfs.verify-checksum", "false")
                .put("hive.dfs.domain-socket-path", "/foo")
                .put("hive.config.resources", "/foo.xml,/bar.xml")
                .put("hive.max-initial-splits", "10")
                .put("hive.max-initial-split-size", "16MB")
                .put("hive.respect-splits.input-formats", "InputFormat1,InputFormat2")
                .put("hive.domain-compaction-threshold", "42")
                .put("hive.recursive-directories", "true")
                .put("hive.storage-format", "SEQUENCEFILE")
                .put("hive.compression-codec", "NONE")
                .put("hive.respect-table-format", "false")
                .put("hive.immutable-partitions", "true")
                .put("hive.max-partitions-per-writers", "222")
                .put("hive.force-local-scheduling", "true")
                .put("hive.max-concurrent-file-renames", "100")
                .put("hive.assume-canonical-partition-keys", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.orc.use-column-names", "true")
                .put("hive.s3.aws-access-key", "abc123")
                .put("hive.s3.aws-secret-key", "secret")
                .put("hive.s3.endpoint", "endpoint.example.com")
                .put("hive.s3.signer-type", "S3SignerType")
                .put("hive.s3.use-instance-credentials", "false")
                .put("hive.s3.ssl.enabled", "false")
                .put("hive.s3.sse.enabled", "true")
                .put("hive.s3.encryption-materials-provider", "EMP_CLASS")
                .put("hive.s3.kms-key-id", "KEY_ID")
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
                .put("hive.s3.pin-client-to-current-region", "true")
                .put("hive.s3.user-agent-prefix", "user-agent-prefix")
                .put("hive.parquet-predicate-pushdown.enabled", "true")
                .put("hive.parquet-optimized-reader.enabled", "true")
                .put("hive.orc.bloom-filters.enabled", "true")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .put("hive.rcfile-optimized-reader.enabled", "true")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM")
                .put("hive.metastore.client.principal", "metastore@EXAMPLE.COM")
                .put("hive.metastore.client.keytab", "/tmp/metastore.keytab")
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "true")
                .put("hive.hdfs.presto.principal", "presto@EXAMPLE.COM")
                .put("hive.hdfs.presto.keytab", "/tmp/presto.keytab")
                .put("hive.skip-deletion-for-alter", "true")
                .put("hive.bucket-execution", "false")
                .put("hive.bucket-writing", "false")
                .put("hive.fs.cache.max-size", "1010")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setTimeZone(nonDefaultTimeZone().toTimeZone().getID())
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(123)
                .setMaxOutstandingSplits(10)
                .setMaxSplitIteratorThreads(10)
                .setAllowCorruptWritesForTesting(true)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMetastoreCacheMaximumSize(5000)
                .setPerTransactionMetastoreCacheMaximumSize(500)
                .setMaxMetastoreRefreshThreads(2500)
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setMaxInitialSplits(10)
                .setMaxInitialSplitSize(new DataSize(16, Unit.MEGABYTE))
                .setDomainCompactionThreshold(42)
                .setForceLocalScheduling(true)
                .setMaxConcurrentFileRenames(100)
                .setRecursiveDirWalkerEnabled(true)
                .setIpcPingInterval(new Duration(34, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setVerifyChecksum(false)
                .setResourceConfigFiles(ImmutableList.of("/foo.xml", "/bar.xml"))
                .setHiveStorageFormat(HiveStorageFormat.SEQUENCEFILE)
                .setHiveCompressionCodec(HiveCompressionCodec.NONE)
                .setRespectSplitsInputFormats(ImmutableSet.of("InputFormat1", "InputFormat2"))
                .setRespectTableFormat(false)
                .setImmutablePartitions(true)
                .setMaxPartitionsPerWriter(222)
                .setDomainSocketPath("/foo")
                .setUseParquetColumnNames(true)
                .setUseOrcColumnNames(true)
                .setS3AwsAccessKey("abc123")
                .setS3AwsSecretKey("secret")
                .setS3Endpoint("endpoint.example.com")
                .setS3SignerType(PrestoS3SignerType.S3SignerType)
                .setS3UseInstanceCredentials(false)
                .setS3SslEnabled(false)
                .setS3SseEnabled(true)
                .setS3EncryptionMaterialsProvider("EMP_CLASS")
                .setS3KmsKeyId("KEY_ID")
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
                .setPinS3ClientToCurrentRegion(true)
                .setS3UserAgentPrefix("user-agent-prefix")
                .setParquetPredicatePushdownEnabled(true)
                .setParquetOptimizedReaderEnabled(true)
                .setAssumeCanonicalPartitionKeys(true)
                .setOrcBloomFiltersEnabled(true)
                .setOrcMaxMergeDistance(new DataSize(22, Unit.KILOBYTE))
                .setOrcMaxBufferSize(new DataSize(44, Unit.KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(55, Unit.KILOBYTE))
                .setRcfileOptimizedReaderEnabled(true)
                .setHiveMetastoreAuthenticationType(HiveClientConfig.HiveMetastoreAuthenticationType.KERBEROS)
                .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM")
                .setHiveMetastoreClientKeytab("/tmp/metastore.keytab")
                .setHdfsAuthenticationType(HiveClientConfig.HdfsAuthenticationType.KERBEROS)
                .setHdfsImpersonationEnabled(true)
                .setHdfsPrestoPrincipal("presto@EXAMPLE.COM")
                .setHdfsPrestoKeytab("/tmp/presto.keytab")
                .setSkipDeletionForAlter(true)
                .setBucketExecutionEnabled(false)
                .setBucketWritingEnabled(false)
                .setFileSystemMaxCacheSize(1010);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
