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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;

public class HiveCommonClientConfig
{
    private NodeSelectionStrategy nodeSelectionStrategy = NO_PREFERENCE;
    private boolean orcBloomFiltersEnabled;
    private boolean orcLazyReadSmallRanges = true;
    private DataSize orcMaxBufferSize = new DataSize(8, MEGABYTE);
    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);
    private DataSize orcMaxReadBlockSize = new DataSize(16, MEGABYTE);
    private boolean orcOptimizedWriterEnabled = true;
    private DataSize orcStreamBufferSize = new DataSize(8, MEGABYTE);
    private OrcWriteValidationMode orcWriterValidationMode = OrcWriteValidationMode.BOTH;
    private double orcWriterValidationPercentage;
    private boolean useOrcColumnNames;
    private DataSize orcTinyStripeThreshold = new DataSize(8, MEGABYTE);
    private boolean parquetBatchReadOptimizationEnabled;
    private boolean parquetEnableBatchReaderVerification;
    private DataSize parquetMaxReadBlockSize = new DataSize(16, MEGABYTE);
    private boolean rangeFiltersOnSubscriptsEnabled;
    private boolean readNullMaskedParquetEncryptedValueEnabled;
    private boolean useParquetColumnNames;
    private boolean zstdJniDecompressionEnabled;
    private String catalogName;
    private DataSize affinitySchedulingFileSectionSize = new DataSize(256, MEGABYTE);

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @Config("hive.node-selection-strategy")
    public HiveCommonClientConfig setNodeSelectionStrategy(NodeSelectionStrategy nodeSelectionStrategy)
    {
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        return this;
    }

    public boolean isOrcBloomFiltersEnabled()
    {
        return orcBloomFiltersEnabled;
    }

    @Config("hive.orc.bloom-filters.enabled")
    public HiveCommonClientConfig setOrcBloomFiltersEnabled(boolean orcBloomFiltersEnabled)
    {
        this.orcBloomFiltersEnabled = orcBloomFiltersEnabled;
        return this;
    }

    @Deprecated
    public boolean isOrcLazyReadSmallRanges()
    {
        return orcLazyReadSmallRanges;
    }

    // TODO remove config option once efficacy is proven
    @Deprecated
    @Config("hive.orc.lazy-read-small-ranges")
    @ConfigDescription("ORC read small disk ranges lazily")
    public HiveCommonClientConfig setOrcLazyReadSmallRanges(boolean orcLazyReadSmallRanges)
    {
        this.orcLazyReadSmallRanges = orcLazyReadSmallRanges;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxBufferSize()
    {
        return orcMaxBufferSize;
    }

    @Config("hive.orc.max-buffer-size")
    public HiveCommonClientConfig setOrcMaxBufferSize(DataSize orcMaxBufferSize)
    {
        this.orcMaxBufferSize = orcMaxBufferSize;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxMergeDistance()
    {
        return orcMaxMergeDistance;
    }

    @Config("hive.orc.max-merge-distance")
    public HiveCommonClientConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
    {
        this.orcMaxMergeDistance = orcMaxMergeDistance;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxReadBlockSize()
    {
        return orcMaxReadBlockSize;
    }

    @Config("hive.orc.max-read-block-size")
    public HiveCommonClientConfig setOrcMaxReadBlockSize(DataSize orcMaxReadBlockSize)
    {
        this.orcMaxReadBlockSize = orcMaxReadBlockSize;
        return this;
    }

    @Deprecated
    public boolean isOrcOptimizedWriterEnabled()
    {
        return orcOptimizedWriterEnabled;
    }

    @Deprecated
    @Config("hive.orc.optimized-writer.enabled")
    public HiveCommonClientConfig setOrcOptimizedWriterEnabled(boolean orcOptimizedWriterEnabled)
    {
        this.orcOptimizedWriterEnabled = orcOptimizedWriterEnabled;
        return this;
    }

    @NotNull
    public DataSize getOrcStreamBufferSize()
    {
        return orcStreamBufferSize;
    }

    @Config("hive.orc.stream-buffer-size")
    public HiveCommonClientConfig setOrcStreamBufferSize(DataSize orcStreamBufferSize)
    {
        this.orcStreamBufferSize = orcStreamBufferSize;
        return this;
    }

    @NotNull
    public OrcWriteValidationMode getOrcWriterValidationMode()
    {
        return orcWriterValidationMode;
    }

    @Config("hive.orc.writer.validation-mode")
    @ConfigDescription("Level of detail in ORC validation. Lower levels require more memory.")
    public HiveCommonClientConfig setOrcWriterValidationMode(OrcWriteValidationMode orcWriterValidationMode)
    {
        this.orcWriterValidationMode = orcWriterValidationMode;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    public double getOrcWriterValidationPercentage()
    {
        return orcWriterValidationPercentage;
    }

    @Config("hive.orc.writer.validation-percentage")
    @ConfigDescription("Percentage of ORC files to validate after write by re-reading the whole file")
    public HiveCommonClientConfig setOrcWriterValidationPercentage(double orcWriterValidationPercentage)
    {
        this.orcWriterValidationPercentage = orcWriterValidationPercentage;
        return this;
    }

    public boolean isUseOrcColumnNames()
    {
        return useOrcColumnNames;
    }

    @Config("hive.orc.use-column-names")
    @ConfigDescription("Access ORC columns using names from the file first, and fallback to Hive schema column names if not found to ensure backward compatibility with old data")
    public HiveCommonClientConfig setUseOrcColumnNames(boolean useOrcColumnNames)
    {
        this.useOrcColumnNames = useOrcColumnNames;
        return this;
    }

    @NotNull
    public DataSize getOrcTinyStripeThreshold()
    {
        return orcTinyStripeThreshold;
    }

    @Config("hive.orc.tiny-stripe-threshold")
    public HiveCommonClientConfig setOrcTinyStripeThreshold(DataSize orcTinyStripeThreshold)
    {
        this.orcTinyStripeThreshold = orcTinyStripeThreshold;
        return this;
    }

    @Config("hive.parquet-batch-read-optimization-enabled")
    @ConfigDescription("enable parquet batch reads optimization")
    public HiveCommonClientConfig setParquetBatchReadOptimizationEnabled(boolean parquetBatchReadOptimizationEnabled)
    {
        this.parquetBatchReadOptimizationEnabled = parquetBatchReadOptimizationEnabled;
        return this;
    }

    public boolean isParquetBatchReadOptimizationEnabled()
    {
        return this.parquetBatchReadOptimizationEnabled;
    }

    @Config("hive.enable-parquet-batch-reader-verification")
    @ConfigDescription("enable optimized parquet reader")
    public HiveCommonClientConfig setParquetBatchReaderVerificationEnabled(boolean parquetEnableBatchReaderVerification)
    {
        this.parquetEnableBatchReaderVerification = parquetEnableBatchReaderVerification;
        return this;
    }

    public boolean isParquetBatchReaderVerificationEnabled()
    {
        return this.parquetEnableBatchReaderVerification;
    }

    @NotNull
    public DataSize getParquetMaxReadBlockSize()
    {
        return parquetMaxReadBlockSize;
    }

    @Config("hive.parquet.max-read-block-size")
    public HiveCommonClientConfig setParquetMaxReadBlockSize(DataSize parquetMaxReadBlockSize)
    {
        this.parquetMaxReadBlockSize = parquetMaxReadBlockSize;
        return this;
    }

    public boolean isRangeFiltersOnSubscriptsEnabled()
    {
        return rangeFiltersOnSubscriptsEnabled;
    }

    @Config("hive.range-filters-on-subscripts-enabled")
    @ConfigDescription("Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers")
    public HiveCommonClientConfig setRangeFiltersOnSubscriptsEnabled(boolean rangeFiltersOnSubscriptsEnabled)
    {
        this.rangeFiltersOnSubscriptsEnabled = rangeFiltersOnSubscriptsEnabled;
        return this;
    }

    @Config("hive.read-null-masked-parquet-encrypted-value-enabled")
    @ConfigDescription("Read null masked value when access is denied for an encrypted parquet column")
    public HiveCommonClientConfig setReadNullMaskedParquetEncryptedValue(boolean readNullMaskedParquetEncryptedValueEnabled)
    {
        this.readNullMaskedParquetEncryptedValueEnabled = readNullMaskedParquetEncryptedValueEnabled;
        return this;
    }

    public boolean getReadNullMaskedParquetEncryptedValue()
    {
        return this.readNullMaskedParquetEncryptedValueEnabled;
    }

    public boolean isUseParquetColumnNames()
    {
        return useParquetColumnNames;
    }

    @Config("hive.parquet.use-column-names")
    @ConfigDescription("Access Parquet columns using names from the file")
    public HiveCommonClientConfig setUseParquetColumnNames(boolean useParquetColumnNames)
    {
        this.useParquetColumnNames = useParquetColumnNames;
        return this;
    }

    public boolean isZstdJniDecompressionEnabled()
    {
        return zstdJniDecompressionEnabled;
    }

    @Config("hive.zstd-jni-decompression-enabled")
    public HiveCommonClientConfig setZstdJniDecompressionEnabled(boolean zstdJniDecompressionEnabled)
    {
        this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
        return this;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    @Config("hive.metastore.catalog.name")
    @ConfigDescription("Specified property to store the metastore catalog name.")
    public HiveCommonClientConfig setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    @NotNull
    public DataSize getAffinitySchedulingFileSectionSize()
    {
        return affinitySchedulingFileSectionSize;
    }

    @Config("hive.affinity-scheduling-file-section-size")
    public HiveCommonClientConfig setAffinitySchedulingFileSectionSize(DataSize affinitySchedulingFileSectionSize)
    {
        this.affinitySchedulingFileSectionSize = affinitySchedulingFileSectionSize;
        return this;
    }
}
