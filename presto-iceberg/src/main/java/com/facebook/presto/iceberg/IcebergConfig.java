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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.LegacyConfig;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.hadoop.HadoopFileIO;

import java.util.EnumSet;
import java.util.List;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.airlift.units.DataSize.succinctDataSize;
import static com.facebook.presto.hive.HiveCompressionCodec.ZSTD;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergFileFormat.PARQUET;
import static com.facebook.presto.iceberg.util.StatisticsUtil.decodeMergeFlags;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT;
import static org.apache.iceberg.TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;

public class IcebergConfig
{
    private IcebergFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = ZSTD;
    private CatalogType catalogType = HIVE;
    private String catalogWarehouse;
    private String catalogWarehouseDataDir;
    private int catalogCacheSize = 10;
    private int maxPartitionsPerWriter = 100;
    private List<String> hadoopConfigResources = ImmutableList.of();
    private double minimumAssignedSplitWeight = 0.05;
    private boolean parquetDereferencePushdownEnabled = true;
    private boolean mergeOnReadModeEnabled = true;
    private double statisticSnapshotRecordDifferenceWeight;
    private boolean pushdownFilterEnabled;
    private boolean deleteAsJoinRewriteEnabled = true;
    private int deleteAsJoinRewriteMaxDeleteColumns = 400;
    private int rowsForMetadataOptimizationThreshold = 1000;
    private int metadataPreviousVersionsMax = METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT;
    private boolean metadataDeleteAfterCommit = METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT;
    private int metricsMaxInferredColumn = METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;
    private int statisticsKllSketchKParameter = 1024;

    private EnumSet<ColumnStatisticType> hiveStatisticsMergeFlags = EnumSet.noneOf(ColumnStatisticType.class);
    private String fileIOImpl = HadoopFileIO.class.getName();
    private boolean manifestCachingEnabled = true;
    private long maxManifestCacheSize = IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT;
    private long manifestCacheExpireDuration = IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT;
    private long manifestCacheMaxContentLength = IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT;
    private DataSize manifestCacheMaxChunkSize = succinctDataSize(2, MEGABYTE);
    private int splitManagerThreads = Runtime.getRuntime().availableProcessors();
    private DataSize maxStatisticsFileCacheSize = succinctDataSize(256, MEGABYTE);
    private String materializedViewStoragePrefix = "__mv_storage__";

    @NotNull
    public FileFormat getFileFormat()
    {
        return FileFormat.valueOf(fileFormat.name());
    }

    @Config("iceberg.file-format")
    public IcebergConfig setFileFormat(IcebergFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("iceberg.compression-codec")
    public IcebergConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    @NotNull
    public CatalogType getCatalogType()
    {
        return catalogType;
    }

    @Config("iceberg.catalog.type")
    @ConfigDescription("Iceberg catalog type")
    public IcebergConfig setCatalogType(CatalogType catalogType)
    {
        this.catalogType = catalogType;
        return this;
    }

    public String getCatalogWarehouse()
    {
        return catalogWarehouse;
    }

    @Config("iceberg.catalog.warehouse")
    @ConfigDescription("Iceberg catalog warehouse root path")
    public IcebergConfig setCatalogWarehouse(String catalogWarehouse)
    {
        this.catalogWarehouse = catalogWarehouse;
        return this;
    }

    public String getCatalogWarehouseDataDir()
    {
        return catalogWarehouseDataDir;
    }

    @Config("iceberg.catalog.hadoop.warehouse.datadir")
    @ConfigDescription("Iceberg catalog default root data writing directory. This is only supported with Hadoop catalog.")
    public IcebergConfig setCatalogWarehouseDataDir(String catalogWarehouseDataDir)
    {
        this.catalogWarehouseDataDir = catalogWarehouseDataDir;
        return this;
    }

    @Min(1)
    public int getCatalogCacheSize()
    {
        return catalogCacheSize;
    }

    @Config("iceberg.catalog.cached-catalog-num")
    @ConfigDescription("number of Iceberg catalog to cache across all sessions")
    public IcebergConfig setCatalogCacheSize(int catalogCacheSize)
    {
        this.catalogCacheSize = catalogCacheSize;
        return this;
    }

    public List<String> getHadoopConfigResources()
    {
        return hadoopConfigResources;
    }

    @Config("iceberg.hadoop.config.resources")
    @ConfigDescription("Comma separated paths to Hadoop configuration resource files")
    public IcebergConfig setHadoopConfigResources(String files)
    {
        if (files != null) {
            this.hadoopConfigResources = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(files);
        }
        return this;
    }

    @Min(1)
    public int getMaxPartitionsPerWriter()
    {
        return maxPartitionsPerWriter;
    }

    @Config("iceberg.max-partitions-per-writer")
    @ConfigDescription("Maximum number of partitions per writer")
    public IcebergConfig setMaxPartitionsPerWriter(int maxPartitionsPerWriter)
    {
        this.maxPartitionsPerWriter = maxPartitionsPerWriter;
        return this;
    }

    @Config("iceberg.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned")
    public IcebergConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
    {
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        return this;
    }

    @DecimalMax("1")
    @DecimalMin(value = "0", inclusive = false)
    public double getMinimumAssignedSplitWeight()
    {
        return minimumAssignedSplitWeight;
    }

    @Config("iceberg.enable-parquet-dereference-pushdown")
    @ConfigDescription("enable parquet dereference pushdown")
    public IcebergConfig setParquetDereferencePushdownEnabled(boolean parquetDereferencePushdownEnabled)
    {
        this.parquetDereferencePushdownEnabled = parquetDereferencePushdownEnabled;
        return this;
    }

    public boolean isParquetDereferencePushdownEnabled()
    {
        return parquetDereferencePushdownEnabled;
    }

    @Config("iceberg.enable-merge-on-read-mode")
    @ConfigDescription("enable merge-on-read mode")
    public IcebergConfig setMergeOnReadModeEnabled(boolean mergeOnReadModeEnabled)
    {
        this.mergeOnReadModeEnabled = mergeOnReadModeEnabled;
        return this;
    }

    public boolean isMergeOnReadModeEnabled()
    {
        return mergeOnReadModeEnabled;
    }

    @Config("iceberg.hive-statistics-merge-strategy")
    @ConfigDescription("Comma separated list of statistics to use from the Hive metastore to override iceberg table statistics")
    public IcebergConfig setHiveStatisticsMergeFlags(String mergeFlags)
    {
        this.hiveStatisticsMergeFlags = decodeMergeFlags(mergeFlags);
        return this;
    }

    public EnumSet<ColumnStatisticType> getHiveStatisticsMergeFlags()
    {
        return hiveStatisticsMergeFlags;
    }

    @Config("iceberg.statistic-snapshot-record-difference-weight")
    @ConfigDescription("the amount that the difference in total record count matters when " +
            "calculating the closest snapshot when picking statistics. A value of 1 means a single " +
            "record is equivalent to 1 millisecond of time difference.")
    public IcebergConfig setStatisticSnapshotRecordDifferenceWeight(double weight)
    {
        this.statisticSnapshotRecordDifferenceWeight = weight;
        return this;
    }

    public double getStatisticSnapshotRecordDifferenceWeight()
    {
        return statisticSnapshotRecordDifferenceWeight;
    }

    @Config("iceberg.pushdown-filter-enabled")
    @ConfigDescription("Experimental: Enable filter pushdown for Iceberg. This is only supported with Native Worker.")
    public IcebergConfig setPushdownFilterEnabled(boolean pushdownFilterEnabled)
    {
        this.pushdownFilterEnabled = pushdownFilterEnabled;
        return this;
    }

    public boolean isPushdownFilterEnabled()
    {
        return pushdownFilterEnabled;
    }

    @LegacyConfig(value = "iceberg.delete-as-join-rewrite-enabled")
    @Config("deprecated.iceberg.delete-as-join-rewrite-enabled")
    @ConfigDescription("When enabled, equality delete row filtering will be implemented by rewriting the query plan to join with the delete keys. " +
            "Deprecated: Set 'iceberg.delete-as-join-rewrite-max-delete-columns' to 0 to control the enabling of this feature.  This will be removed in a future release.")
    @Deprecated
    public IcebergConfig setDeleteAsJoinRewriteEnabled(boolean deleteAsJoinPushdownEnabled)
    {
        this.deleteAsJoinRewriteEnabled = deleteAsJoinPushdownEnabled;
        return this;
    }

    @Deprecated
    public boolean isDeleteAsJoinRewriteEnabled()
    {
        return deleteAsJoinRewriteEnabled;
    }

    @Config("iceberg.delete-as-join-rewrite-max-delete-columns")
    @ConfigDescription("The maximum number of columns that can be used in a delete as join rewrite. " +
            "If the number of columns exceeds this value, the delete as join rewrite will not be applied.")
    @Min(0)
    @Max(400)
    public IcebergConfig setDeleteAsJoinRewriteMaxDeleteColumns(int deleteAsJoinRewriteMaxDeleteColumns)
    {
        this.deleteAsJoinRewriteMaxDeleteColumns = deleteAsJoinRewriteMaxDeleteColumns;
        return this;
    }

    public int getDeleteAsJoinRewriteMaxDeleteColumns()
    {
        return deleteAsJoinRewriteMaxDeleteColumns;
    }

    @Config("iceberg.rows-for-metadata-optimization-threshold")
    @ConfigDescription("The max partitions number to utilize metadata optimization. 0 means skip the metadata optimization directly.")
    public IcebergConfig setRowsForMetadataOptimizationThreshold(int rowsForMetadataOptimizationThreshold)
    {
        this.rowsForMetadataOptimizationThreshold = rowsForMetadataOptimizationThreshold;
        return this;
    }

    @Min(0)
    public int getRowsForMetadataOptimizationThreshold()
    {
        return rowsForMetadataOptimizationThreshold;
    }

    public boolean getManifestCachingEnabled()
    {
        return manifestCachingEnabled;
    }

    @Config("iceberg.io.manifest.cache-enabled")
    @ConfigDescription("Enable/disable the manifest caching feature")
    public IcebergConfig setManifestCachingEnabled(boolean manifestCachingEnabled)
    {
        this.manifestCachingEnabled = manifestCachingEnabled;
        return this;
    }

    public String getFileIOImpl()
    {
        return fileIOImpl;
    }

    @NotNull
    @Config("iceberg.io-impl")
    @ConfigDescription("Custom FileIO implementation to use in a catalog")
    public IcebergConfig setFileIOImpl(String fileIOImpl)
    {
        this.fileIOImpl = fileIOImpl;
        return this;
    }

    public long getMaxManifestCacheSize()
    {
        return maxManifestCacheSize;
    }

    @Min(1)
    @Config("iceberg.io.manifest.cache.max-total-bytes")
    @ConfigDescription("Maximum total amount of bytes to cache in the manifest cache")
    public IcebergConfig setMaxManifestCacheSize(long maxManifestCacheSize)
    {
        this.maxManifestCacheSize = maxManifestCacheSize;
        return this;
    }

    public long getManifestCacheExpireDuration()
    {
        return manifestCacheExpireDuration;
    }

    @Min(0)
    @Config("iceberg.io.manifest.cache.expiration-interval-ms")
    @ConfigDescription("Maximum duration for which an entry stays in the manifest cache")
    public IcebergConfig setManifestCacheExpireDuration(long manifestCacheExpireDuration)
    {
        this.manifestCacheExpireDuration = manifestCacheExpireDuration;
        return this;
    }

    public long getManifestCacheMaxContentLength()
    {
        return manifestCacheMaxContentLength;
    }

    @Min(0)
    @Config("iceberg.io.manifest.cache.max-content-length")
    @ConfigDescription("Maximum length of a manifest file to be considered for caching in bytes")
    public IcebergConfig setManifestCacheMaxContentLength(long manifestCacheMaxContentLength)
    {
        this.manifestCacheMaxContentLength = manifestCacheMaxContentLength;
        return this;
    }

    public DataSize getManifestCacheMaxChunkSize()
    {
        return manifestCacheMaxChunkSize;
    }

    @Min(1024)
    @Config("iceberg.io.manifest.cache.max-chunk-size")
    @ConfigDescription("Maximum length of a buffer used to cache manifest file content. Only applicable to HIVE catalog.")
    public IcebergConfig setManifestCacheMaxChunkSize(DataSize manifestCacheMaxChunkSize)
    {
        this.manifestCacheMaxChunkSize = manifestCacheMaxChunkSize;
        return this;
    }

    @Min(0)
    public int getSplitManagerThreads()
    {
        return splitManagerThreads;
    }

    @Config("iceberg.split-manager-threads")
    @ConfigDescription("Number of threads to use for generating splits")
    public IcebergConfig setSplitManagerThreads(int splitManagerThreads)
    {
        this.splitManagerThreads = splitManagerThreads;
        return this;
    }

    @Min(0)
    public int getMetadataPreviousVersionsMax()
    {
        return metadataPreviousVersionsMax;
    }

    @Config("iceberg.metadata-previous-versions-max")
    @ConfigDescription("The max number of old metadata files to keep in metadata log")
    public IcebergConfig setMetadataPreviousVersionsMax(int metadataPreviousVersionsMax)
    {
        this.metadataPreviousVersionsMax = metadataPreviousVersionsMax;
        return this;
    }

    public boolean isMetadataDeleteAfterCommit()
    {
        return metadataDeleteAfterCommit;
    }

    @Config("iceberg.metadata-delete-after-commit")
    @ConfigDescription("Whether enables to delete the oldest metadata file after commit")
    public IcebergConfig setMetadataDeleteAfterCommit(boolean metadataDeleteAfterCommit)
    {
        this.metadataDeleteAfterCommit = metadataDeleteAfterCommit;
        return this;
    }

    public int getMetricsMaxInferredColumn()
    {
        return metricsMaxInferredColumn;
    }

    @Config("iceberg.metrics-max-inferred-column")
    @ConfigDescription("The maximum number of columns for which metrics are collected")
    public IcebergConfig setMetricsMaxInferredColumn(int metricsMaxInferredColumn)
    {
        this.metricsMaxInferredColumn = metricsMaxInferredColumn;
        return this;
    }

    public DataSize getMaxStatisticsFileCacheSize()
    {
        return maxStatisticsFileCacheSize;
    }

    @Config("iceberg.max-statistics-file-cache-size")
    @ConfigDescription("The maximum size in bytes the statistics file cache should consume")
    public IcebergConfig setMaxStatisticsFileCacheSize(DataSize maxStatisticsFileCacheSize)
    {
        this.maxStatisticsFileCacheSize = maxStatisticsFileCacheSize;
        return this;
    }

    public int getStatisticsKllSketchKParameter()
    {
        return this.statisticsKllSketchKParameter;
    }

    @Config("iceberg.statistics-kll-sketch-k-parameter")
    @Min(8)
    @Max(65535)
    @ConfigDescription("K parameter for KLL sketches when generating histogram statistics")
    public IcebergConfig setStatisticsKllSketchKParameter(int kllSketchKParameter)
    {
        this.statisticsKllSketchKParameter = kllSketchKParameter;
        return this;
    }

    @NotNull
    public String getMaterializedViewStoragePrefix()
    {
        return materializedViewStoragePrefix;
    }

    @Config("iceberg.materialized-view-storage-prefix")
    @ConfigDescription("Default prefix for generated materialized view storage table names. " +
            "This is only used when the storage_table table property is not explicitly set. " +
            "When a custom table name is provided, it takes precedence over this prefix.")
    public IcebergConfig setMaterializedViewStoragePrefix(String materializedViewStoragePrefix)
    {
        this.materializedViewStoragePrefix = materializedViewStoragePrefix;
        return this;
    }
}
