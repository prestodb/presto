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
import com.facebook.presto.hive.HiveCompressionCodec;
import org.apache.iceberg.FileFormat;

import javax.validation.constraints.NotNull;

import static com.facebook.presto.hive.HiveCompressionCodec.GZIP;
import static com.facebook.presto.iceberg.IcebergFileFormat.PARQUET;

public class IcebergConfig
{
    private IcebergFileFormat fileFormat = PARQUET;
    private HiveCompressionCodec compressionCodec = GZIP;

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
}
