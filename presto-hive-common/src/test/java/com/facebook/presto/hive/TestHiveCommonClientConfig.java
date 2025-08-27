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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;

public class TestHiveCommonClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveCommonClientConfig.class)
                .setRangeFiltersOnSubscriptsEnabled(false)
                .setNodeSelectionStrategy(NodeSelectionStrategy.valueOf("NO_PREFERENCE"))
                .setUseParquetColumnNames(false)
                .setParquetMaxReadBlockSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setOrcBloomFiltersEnabled(false)
                .setOrcMaxMergeDistance(new DataSize(1, DataSize.Unit.MEGABYTE))
                .setOrcMaxBufferSize(new DataSize(8, DataSize.Unit.MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, DataSize.Unit.MEGABYTE))
                .setOrcTinyStripeThreshold(new DataSize(8, DataSize.Unit.MEGABYTE))
                .setOrcMaxReadBlockSize(new DataSize(16, DataSize.Unit.MEGABYTE))
                .setOrcLazyReadSmallRanges(true)
                .setOrcOptimizedWriterEnabled(true)
                .setOrcWriterValidationPercentage(0.0)
                .setOrcWriterValidationMode(OrcWriteValidation.OrcWriteValidationMode.BOTH)
                .setUseOrcColumnNames(false)
                .setZstdJniDecompressionEnabled(false)
                .setParquetBatchReaderVerificationEnabled(false)
                .setParquetBatchReadOptimizationEnabled(false)
                .setReadNullMaskedParquetEncryptedValue(false)
                .setCatalogName(null)
                .setAffinitySchedulingFileSectionSize(new DataSize(256, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.range-filters-on-subscripts-enabled", "true")
                .put("hive.node-selection-strategy", "HARD_AFFINITY")
                .put("hive.parquet.use-column-names", "true")
                .put("hive.parquet.max-read-block-size", "66kB")
                .put("hive.orc.bloom-filters.enabled", "true")
                .put("hive.orc.max-merge-distance", "22kB")
                .put("hive.orc.max-buffer-size", "44kB")
                .put("hive.orc.stream-buffer-size", "55kB")
                .put("hive.orc.tiny-stripe-threshold", "61kB")
                .put("hive.orc.max-read-block-size", "66kB")
                .put("hive.orc.lazy-read-small-ranges", "false")
                .put("hive.orc.optimized-writer.enabled", "false")
                .put("hive.orc.writer.validation-percentage", "0.16")
                .put("hive.orc.writer.validation-mode", "DETAILED")
                .put("hive.orc.use-column-names", "true")
                .put("hive.zstd-jni-decompression-enabled", "true")
                .put("hive.enable-parquet-batch-reader-verification", "true")
                .put("hive.parquet-batch-read-optimization-enabled", "true")
                .put("hive.read-null-masked-parquet-encrypted-value-enabled", "true")
                .put("hive.metastore.catalog.name", "catalogName")
                .put("hive.affinity-scheduling-file-section-size", "512MB")
                .build();

        HiveCommonClientConfig expected = new HiveCommonClientConfig()
                .setRangeFiltersOnSubscriptsEnabled(true)
                .setNodeSelectionStrategy(HARD_AFFINITY)
                .setUseParquetColumnNames(true)
                .setParquetMaxReadBlockSize(new DataSize(66, DataSize.Unit.KILOBYTE))
                .setOrcBloomFiltersEnabled(true)
                .setOrcMaxMergeDistance(new DataSize(22, DataSize.Unit.KILOBYTE))
                .setOrcMaxBufferSize(new DataSize(44, DataSize.Unit.KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(55, DataSize.Unit.KILOBYTE))
                .setOrcTinyStripeThreshold(new DataSize(61, DataSize.Unit.KILOBYTE))
                .setOrcMaxReadBlockSize(new DataSize(66, DataSize.Unit.KILOBYTE))
                .setOrcLazyReadSmallRanges(false)
                .setOrcOptimizedWriterEnabled(false)
                .setOrcWriterValidationPercentage(0.16)
                .setOrcWriterValidationMode(OrcWriteValidation.OrcWriteValidationMode.DETAILED)
                .setUseOrcColumnNames(true)
                .setZstdJniDecompressionEnabled(true)
                .setParquetBatchReaderVerificationEnabled(true)
                .setParquetBatchReadOptimizationEnabled(true)
                .setReadNullMaskedParquetEncryptedValue(true)
                .setCatalogName("catalogName")
                .setAffinitySchedulingFileSectionSize(new DataSize(512, MEGABYTE));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
