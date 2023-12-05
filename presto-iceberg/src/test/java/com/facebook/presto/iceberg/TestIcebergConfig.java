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

import com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy;
import com.google.common.collect.ImmutableMap;
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
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NDV;

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
                .setHiveStatisticsMergeStrategy(HiveStatisticsMergeStrategy.NONE)
                .setStatisticSnapshotRecordDifferenceWeight(0.0)
                .setMaxPartitionsPerWriter(100)
                .setMinimumAssignedSplitWeight(0.05)
                .setParquetDereferencePushdownEnabled(true)
                .setMergeOnReadModeEnabled(true)
                .setPushdownFilterEnabled(false));
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
                .put("iceberg.hive-statistics-merge-strategy", "USE_NDV")
                .put("iceberg.pushdown-filter-enabled", "true")
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
                .setHiveStatisticsMergeStrategy(USE_NDV)
                .setPushdownFilterEnabled(true);

        assertFullMapping(properties, expected);
    }
}
