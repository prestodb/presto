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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.FileFormat;

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
    private List<String> hadoopConfigResources = ImmutableList.of();

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
}
