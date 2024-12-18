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

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * This wrapper class is added to avoid adding special classes and simplifying
 * the presto protocol generation for Prestissimo
 */
public enum FileFormat
{
    ORC("orc", true),
    PARQUET("parquet", true),
    AVRO("avro", true),
    METADATA("metadata.json", false);

    private final String ext;
    private final boolean splittable;

    FileFormat(String ext, boolean splittable)
    {
        this.ext = "." + ext;
        this.splittable = splittable;
    }

    public String addExtension(String filename)
    {
        if (filename.endsWith(ext)) {
            return filename;
        }
        return filename + ext;
    }

    public static FileFormat fromIcebergFileFormat(org.apache.iceberg.FileFormat format)
    {
        FileFormat prestoFileFormat;
        switch (format) {
            case ORC:
                prestoFileFormat = ORC;
                break;
            case PARQUET:
                prestoFileFormat = PARQUET;
                break;
            case AVRO:
                prestoFileFormat = AVRO;
                break;
            case METADATA:
                prestoFileFormat = METADATA;
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported file format: " + format);
        }

        return prestoFileFormat;
    }

    public org.apache.iceberg.FileFormat toIceberg()
    {
        org.apache.iceberg.FileFormat fileFormat;
        switch (this) {
            case ORC:
                fileFormat = org.apache.iceberg.FileFormat.ORC;
                break;
            case PARQUET:
                fileFormat = org.apache.iceberg.FileFormat.PARQUET;
                break;
            case AVRO:
                fileFormat = org.apache.iceberg.FileFormat.AVRO;
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported file format: " + this);
        }
        return fileFormat;
    }
}
