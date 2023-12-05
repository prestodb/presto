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
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.FileFormat;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.List;

import static com.facebook.presto.hive.HiveCompressionCodec.GZIP;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergFileFormat.PARQUET;

public class IcebergConfig
{
    private IcebergFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = GZIP;
    private CatalogType catalogType = HIVE;
    private String catalogWarehouse;
    private int catalogCacheSize = 10;
    private int maxPartitionsPerWriter = 100;
    private List<String> hadoopConfigResources = ImmutableList.of();
    private double minimumAssignedSplitWeight = 0.05;
    private boolean parquetDereferencePushdownEnabled = true;
    private boolean mergeOnReadModeEnabled = true;
    private double statisticSnapshotRecordDifferenceWeight;
    private boolean pushdownFilterEnabled;

    private HiveStatisticsMergeStrategy hiveStatisticsMergeStrategy = HiveStatisticsMergeStrategy.NONE;

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
    @ConfigDescription("determines how to merge statistics that are stored in the Hive Metastore")
    public IcebergConfig setHiveStatisticsMergeStrategy(HiveStatisticsMergeStrategy mergeStrategy)
    {
        this.hiveStatisticsMergeStrategy = mergeStrategy;
        return this;
    }

    public HiveStatisticsMergeStrategy getHiveStatisticsMergeStrategy()
    {
        return hiveStatisticsMergeStrategy;
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
}
