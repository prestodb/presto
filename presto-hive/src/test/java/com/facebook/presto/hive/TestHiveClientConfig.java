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

import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.HiveClientConfig.HiveMetastoreAuthenticationType;
import com.facebook.presto.hive.s3.S3FileSystemType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

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
                .setMaxOutstandingSplitsSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxSplitIteratorThreads(1_000)
                .setAllowCorruptWritesForTesting(false)
                .setMetastoreCacheTtl(new Duration(0, TimeUnit.SECONDS))
                .setMetastoreRefreshInterval(new Duration(0, TimeUnit.SECONDS))
                .setMetastoreCacheMaximumSize(10000)
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setMaxMetastoreRefreshThreads(100)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(new DataSize(32, Unit.MEGABYTE))
                .setSplitLoaderConcurrency(4)
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
                .setS3FileSystemType(S3FileSystemType.PRESTO)
                .setResourceConfigFiles((String) null)
                .setHiveStorageFormat(HiveStorageFormat.RCBINARY)
                .setHiveCompressionCodec(HiveCompressionCodec.GZIP)
                .setRespectTableFormat(true)
                .setImmutablePartitions(false)
                .setMaxPartitionsPerWriter(100)
                .setWriteValidationThreads(16)
                .setUseParquetColumnNames(false)
                .setUseOrcColumnNames(false)
                .setParquetPredicatePushdownEnabled(false)
                .setParquetOptimizedReaderEnabled(false)
                .setAssumeCanonicalPartitionKeys(false)
                .setOrcBloomFiltersEnabled(false)
                .setOrcDefaultBloomFilterFpp(0.05)
                .setOrcMaxMergeDistance(new DataSize(1, Unit.MEGABYTE))
                .setOrcMaxBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, Unit.MEGABYTE))
                .setOrcMaxReadBlockSize(new DataSize(16, Unit.MEGABYTE))
                .setOrcLazyReadSmallRanges(true)
                .setRcfileOptimizedWriterEnabled(true)
                .setRcfileWriterValidate(false)
                .setOrcOptimizedWriterEnabled(false)
                .setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType.NONE)
                .setHdfsAuthenticationType(HdfsAuthenticationType.NONE)
                .setHdfsImpersonationEnabled(false)
                .setSkipDeletionForAlter(false)
                .setBucketExecutionEnabled(true)
                .setBucketWritingEnabled(true)
                .setFileSystemMaxCacheSize(1000)
                .setTableStatisticsEnabled(true)
                .setWritesToNonManagedTablesEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.time-zone", nonDefaultTimeZone().getID())
                .put("hive.max-split-size", "256MB")
                .put("hive.max-partitions-per-scan", "123")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-outstanding-splits-size", "32MB")
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
                .put("hive.s3-file-system-type", "EMRFS")
                .put("hive.config.resources", "/foo.xml,/bar.xml")
                .put("hive.max-initial-splits", "10")
                .put("hive.max-initial-split-size", "16MB")
                .put("hive.split-loader-concurrency", "1")
                .put("hive.domain-compaction-threshold", "42")
                .put("hive.recursive-directories", "true")
                .put("hive.storage-format", "SEQUENCEFILE")
                .put("hive.compression-codec", "NONE")
                .put("hive.respect-table-format", "false")
                .put("hive.immutable-partitions", "true")
                .put("hive.max-partitions-per-writers", "222")
                .put("hive.write-validation-threads", "11")
                .put("hive.force-local-scheduling", "true")
                .put("hive.max-concurrent-file-renames", "100")
                .put("hive.assume-canonical-partition-keys", "true")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.orc.use-column-names", "true")
                .put("hive.parquet-predicate-pushdown.enabled", "true")
                .put("hive.parquet-optimized-reader.enabled", "true")
                .put("hive.orc.bloom-filters.enabled", "true")
                .put("hive.orc.default-bloom-filter-fpp", "0.96")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .put("hive.orc.max-read-block-size", "66kB")
                .put("hive.orc.lazy-read-small-ranges", "false")
                .put("hive.rcfile-optimized-writer.enabled", "false")
                .put("hive.rcfile.writer.validate", "true")
                .put("hive.orc.optimized-writer.enabled", "true")
                .put("hive.metastore.authentication.type", "KERBEROS")
                .put("hive.hdfs.authentication.type", "KERBEROS")
                .put("hive.hdfs.impersonation.enabled", "true")
                .put("hive.skip-deletion-for-alter", "true")
                .put("hive.bucket-execution", "false")
                .put("hive.bucket-writing", "false")
                .put("hive.fs.cache.max-size", "1010")
                .put("hive.table-statistics-enabled", "false")
                .put("hive.non-managed-table-writes-enabled", "true")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setTimeZone(nonDefaultTimeZone().toTimeZone().getID())
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(123)
                .setMaxOutstandingSplits(10)
                .setMaxOutstandingSplitsSize(new DataSize(32, Unit.MEGABYTE))
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
                .setSplitLoaderConcurrency(1)
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
                .setRespectTableFormat(false)
                .setImmutablePartitions(true)
                .setMaxPartitionsPerWriter(222)
                .setWriteValidationThreads(11)
                .setDomainSocketPath("/foo")
                .setS3FileSystemType(S3FileSystemType.EMRFS)
                .setUseParquetColumnNames(true)
                .setUseOrcColumnNames(true)
                .setParquetPredicatePushdownEnabled(true)
                .setParquetOptimizedReaderEnabled(true)
                .setAssumeCanonicalPartitionKeys(true)
                .setOrcBloomFiltersEnabled(true)
                .setOrcDefaultBloomFilterFpp(0.96)
                .setOrcMaxMergeDistance(new DataSize(22, Unit.KILOBYTE))
                .setOrcMaxBufferSize(new DataSize(44, Unit.KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(55, Unit.KILOBYTE))
                .setOrcMaxReadBlockSize(new DataSize(66, Unit.KILOBYTE))
                .setOrcLazyReadSmallRanges(false)
                .setRcfileOptimizedWriterEnabled(false)
                .setRcfileWriterValidate(true)
                .setOrcOptimizedWriterEnabled(true)
                .setHiveMetastoreAuthenticationType(HiveMetastoreAuthenticationType.KERBEROS)
                .setHdfsAuthenticationType(HdfsAuthenticationType.KERBEROS)
                .setHdfsImpersonationEnabled(true)
                .setSkipDeletionForAlter(true)
                .setBucketExecutionEnabled(false)
                .setBucketWritingEnabled(false)
                .setFileSystemMaxCacheSize(1010)
                .setTableStatisticsEnabled(false)
                .setWritesToNonManagedTablesEnabled(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
