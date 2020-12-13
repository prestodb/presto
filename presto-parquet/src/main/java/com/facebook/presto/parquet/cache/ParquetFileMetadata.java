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
package com.facebook.presto.parquet.cache;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import static java.util.Objects.requireNonNull;

public class ParquetFileMetadata
{
    private final ParquetMetadata parquetMetadata;
    private final int metadataSize;

    public ParquetFileMetadata(ParquetMetadata parquetMetadata, int metadataSize)
    {
        this.parquetMetadata = requireNonNull(parquetMetadata, "parquetMetadata is null");
        this.metadataSize = metadataSize;
    }

    public int getMetadataSize()
    {
        return metadataSize;
    }

    public ParquetMetadata getParquetMetadata()
    {
        return parquetMetadata;
    }
}
