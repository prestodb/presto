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
package com.facebook.presto.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.hive.HiveCompressionCodec.GZIP;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergFileFormat.ORC;
import static com.facebook.presto.iceberg.IcebergFileFormat.PARQUET;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctDataSize;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;

public class TestIcebergConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergConfig.class)
                .setFileFormat(PARQUET)
                .setCompressionCodec(GZIP)
                .setCatalogType(HIVE)
                .setCatalogWarehouse(null)
                .setCatalogCacheSize(10)
                .setHadoopConfigResources(null)
                .setHiveStatisticsMergeFlags("")
                .setStatisticSnapshotRecordDifferenceWeight(0.0)
                .setMaxPartitionsPerWriter(100)
                .setMinimumAssignedSplitWeight(0.05)
                .setParquetDereferencePushdownEnabled(true)
                .setMergeOnReadModeEnabled(true)
                .setPushdownFilterEnabled(false)
                .setDeleteAsJoinRewriteEnabled(true)
                .setRowsForMetadataOptimizationThreshold(1000)
                .setManifestCachingEnabled(false)
                .setFileIOImpl(HadoopFileIO.class.getName())
                .setMaxManifestCacheSize(IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT)
                .setManifestCacheExpireDuration(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
                .setManifestCacheMaxContentLength(IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT)
                .setSplitManagerThreads(Runtime.getRuntime().availableProcessors())
                .setMetadataPreviousVersionsMax(METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT)
                .setMetadataDeleteAfterCommit(METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT)
                .setMetricsMaxInferredColumn(METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT)
                .setMaxStatisticsFileCacheSize(succinctDataSize(256, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("iceberg.file-format", "ORC")
                .put("iceberg.compression-codec", "NONE")
                .put("iceberg.catalog.type", "HADOOP")
                .put("iceberg.catalog.warehouse", "path")
                .put("iceberg.catalog.cached-catalog-num", "6")
                .put("iceberg.hadoop.config.resources", "/etc/hadoop/conf/core-site.xml")
                .put("iceberg.max-partitions-per-writer", "222")
                .put("iceberg.minimum-assigned-split-weight", "0.01")
                .put("iceberg.enable-parquet-dereference-pushdown", "false")
                .put("iceberg.enable-merge-on-read-mode", "false")
                .put("iceberg.statistic-snapshot-record-difference-weight", "1.0")
                .put("iceberg.hive-statistics-merge-strategy", NUMBER_OF_DISTINCT_VALUES.name() + "," + TOTAL_SIZE_IN_BYTES.name())
                .put("iceberg.pushdown-filter-enabled", "true")
                .put("iceberg.delete-as-join-rewrite-enabled", "false")
                .put("iceberg.rows-for-metadata-optimization-threshold", "500")
                .put("iceberg.io.manifest.cache-enabled", "true")
                .put("iceberg.io-impl", "com.facebook.presto.iceberg.HdfsFileIO")
                .put("iceberg.io.manifest.cache.max-total-bytes", "1048576000")
                .put("iceberg.io.manifest.cache.expiration-interval-ms", "600000")
                .put("iceberg.io.manifest.cache.max-content-length", "10485760")
                .put("iceberg.split-manager-threads", "42")
                .put("iceberg.metadata-previous-versions-max", "1")
                .put("iceberg.metadata-delete-after-commit", "true")
                .put("iceberg.metrics-max-inferred-column", "16")
                .put("iceberg.max-statistics-file-cache-size", "512MB")
                .build();

        IcebergConfig expected = new IcebergConfig()
                .setFileFormat(ORC)
                .setCompressionCodec(NONE)
                .setCatalogType(HADOOP)
                .setCatalogWarehouse("path")
                .setCatalogCacheSize(6)
                .setHadoopConfigResources("/etc/hadoop/conf/core-site.xml")
                .setMaxPartitionsPerWriter(222)
                .setMinimumAssignedSplitWeight(0.01)
                .setStatisticSnapshotRecordDifferenceWeight(1.0)
                .setParquetDereferencePushdownEnabled(false)
                .setMergeOnReadModeEnabled(false)
                .setHiveStatisticsMergeFlags("NUMBER_OF_DISTINCT_VALUES,TOTAL_SIZE_IN_BYTES")
                .setPushdownFilterEnabled(true)
                .setDeleteAsJoinRewriteEnabled(false)
                .setRowsForMetadataOptimizationThreshold(500)
                .setManifestCachingEnabled(true)
                .setFileIOImpl("com.facebook.presto.iceberg.HdfsFileIO")
                .setMaxManifestCacheSize(1048576000)
                .setManifestCacheExpireDuration(600000)
                .setManifestCacheMaxContentLength(10485760)
                .setSplitManagerThreads(42)
                .setMetadataPreviousVersionsMax(1)
                .setMetadataDeleteAfterCommit(true)
                .setMetricsMaxInferredColumn(16)
                .setMaxStatisticsFileCacheSize(succinctDataSize(512, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
