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
package com.facebook.presto.ducklake.catalog;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A visible-at-snapshot row of {@code ducklake_data_file}, with its {@code ducklake_delete_file}
 * (if any), {@code ducklake_file_partition_value} rows and {@code ducklake_file_column_stats}
 * rows attached. {@code path} has already been resolved by the JDBC layer to an absolute
 * location.
 *
 * <p>{@code partitionValues} is keyed by {@code partition_key_index} (matching {@link
 * DuckLakePartitionField#getPartitionKeyIndex()}); the value is {@code Optional.empty()} when
 * {@code ducklake_file_partition_value.partition_value} is SQL NULL, so that a NULL partition
 * value is distinguishable from a partition key with no row at all (which simply has no entry in
 * the map).
 */
public class DuckLakeDataFile
{
    private final long dataFileId;
    private final String path;
    private final String fileFormat;
    private final long recordCount;
    private final long fileSizeBytes;
    private final OptionalLong footerSize;
    private final OptionalLong rowIdStart;
    private final OptionalLong partitionId;
    private final Optional<String> encryptionKey;
    private final OptionalLong mappingId;
    private final OptionalLong partialMax;
    private final Optional<DuckLakeDeleteFile> deleteFile;
    private final Map<Integer, Optional<String>> partitionValues;
    private final Map<Long, DuckLakeFileColumnStats> columnStats;

    public DuckLakeDataFile(
            long dataFileId,
            String path,
            String fileFormat,
            long recordCount,
            long fileSizeBytes,
            OptionalLong footerSize,
            OptionalLong rowIdStart,
            OptionalLong partitionId,
            Optional<String> encryptionKey,
            OptionalLong mappingId,
            OptionalLong partialMax,
            Optional<DuckLakeDeleteFile> deleteFile,
            Map<Integer, Optional<String>> partitionValues,
            Map<Long, DuckLakeFileColumnStats> columnStats)
    {
        this.dataFileId = dataFileId;
        this.path = requireNonNull(path, "path is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.recordCount = recordCount;
        this.fileSizeBytes = fileSizeBytes;
        this.footerSize = requireNonNull(footerSize, "footerSize is null");
        this.rowIdStart = requireNonNull(rowIdStart, "rowIdStart is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        this.mappingId = requireNonNull(mappingId, "mappingId is null");
        this.partialMax = requireNonNull(partialMax, "partialMax is null");
        this.deleteFile = requireNonNull(deleteFile, "deleteFile is null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
        this.columnStats = requireNonNull(columnStats, "columnStats is null");
    }

    public long getDataFileId()
    {
        return dataFileId;
    }

    public String getPath()
    {
        return path;
    }

    public String getFileFormat()
    {
        return fileFormat;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public long getFileSizeBytes()
    {
        return fileSizeBytes;
    }

    public OptionalLong getFooterSize()
    {
        return footerSize;
    }

    public OptionalLong getRowIdStart()
    {
        return rowIdStart;
    }

    public OptionalLong getPartitionId()
    {
        return partitionId;
    }

    public Optional<String> getEncryptionKey()
    {
        return encryptionKey;
    }

    public OptionalLong getMappingId()
    {
        return mappingId;
    }

    public OptionalLong getPartialMax()
    {
        return partialMax;
    }

    public Optional<DuckLakeDeleteFile> getDeleteFile()
    {
        return deleteFile;
    }

    public Map<Integer, Optional<String>> getPartitionValues()
    {
        return partitionValues;
    }

    public Map<Long, DuckLakeFileColumnStats> getColumnStats()
    {
        return columnStats;
    }
}
