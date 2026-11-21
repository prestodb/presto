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
package com.facebook.presto.ducklake.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * A DuckLake positional delete file, corresponding to an Iceberg positional delete file (a file
 * of {@code file_path}, {@code pos} columns): {@code path} names the rows of the data file this
 * split reads that must be skipped, by row position.
 *
 * <p>{@code partialMax}, when present, means the delete file itself embeds {@code
 * _ducklake_internal_snapshot_id}: only the delete rows whose snapshot id is less than or equal
 * to the split's scan snapshot apply, and rows with a greater snapshot id must be ignored when
 * reading (Phase 4 implements this filtering; this class only carries the value).
 */
public class DeleteFile
{
    private final String path;
    private final String format;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final OptionalLong partialMax;

    @JsonCreator
    public DeleteFile(
            @JsonProperty("path") String path,
            @JsonProperty("format") String format,
            @JsonProperty("recordCount") long recordCount,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("partialMax") OptionalLong partialMax)
    {
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.partialMax = requireNonNull(partialMax, "partialMax is null");
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public String getFormat()
    {
        return format;
    }

    @JsonProperty
    public long getRecordCount()
    {
        return recordCount;
    }

    @JsonProperty
    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public OptionalLong getPartialMax()
    {
        return partialMax;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeleteFile that = (DeleteFile) o;
        return recordCount == that.recordCount &&
                fileSizeInBytes == that.fileSizeInBytes &&
                path.equals(that.path) &&
                format.equals(that.format) &&
                partialMax.equals(that.partialMax);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, format, recordCount, fileSizeInBytes, partialMax);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("records", recordCount)
                .toString();
    }
}
