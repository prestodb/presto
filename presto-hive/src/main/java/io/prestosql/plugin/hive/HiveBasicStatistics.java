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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class HiveBasicStatistics
{
    private final OptionalLong fileCount;
    private final OptionalLong rowCount;
    private final OptionalLong inMemoryDataSizeInBytes;
    private final OptionalLong onDiskDataSizeInBytes;

    public static HiveBasicStatistics createEmptyStatistics()
    {
        return new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty());
    }

    public static HiveBasicStatistics createZeroStatistics()
    {
        return new HiveBasicStatistics(0, 0, 0, 0);
    }

    public HiveBasicStatistics(long fileCount, long rowCount, long inMemoryDataSizeInBytes, long onDiskDataSizeInBytes)
    {
        this(OptionalLong.of(fileCount), OptionalLong.of(rowCount), OptionalLong.of(inMemoryDataSizeInBytes), OptionalLong.of(onDiskDataSizeInBytes));
    }

    @JsonCreator
    public HiveBasicStatistics(
            @JsonProperty("fileCount") OptionalLong fileCount,
            @JsonProperty("rowCount") OptionalLong rowCount,
            @JsonProperty("inMemoryDataSizeInBytes") OptionalLong inMemoryDataSizeInBytes,
            @JsonProperty("onDiskDataSizeInBytes") OptionalLong onDiskDataSizeInBytes)
    {
        this.fileCount = requireNonNull(fileCount, "fileCount is null");
        this.rowCount = requireNonNull(rowCount, "rowCount is null");
        this.inMemoryDataSizeInBytes = requireNonNull(inMemoryDataSizeInBytes, "inMemoryDataSizeInBytes is null");
        this.onDiskDataSizeInBytes = requireNonNull(onDiskDataSizeInBytes, "onDiskDataSizeInBytes is null");
    }

    @JsonProperty
    public OptionalLong getFileCount()
    {
        return fileCount;
    }

    @JsonProperty
    public OptionalLong getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public OptionalLong getInMemoryDataSizeInBytes()
    {
        return inMemoryDataSizeInBytes;
    }

    @JsonProperty
    public OptionalLong getOnDiskDataSizeInBytes()
    {
        return onDiskDataSizeInBytes;
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
        HiveBasicStatistics that = (HiveBasicStatistics) o;
        return Objects.equals(fileCount, that.fileCount) &&
                Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(inMemoryDataSizeInBytes, that.inMemoryDataSizeInBytes) &&
                Objects.equals(onDiskDataSizeInBytes, that.onDiskDataSizeInBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileCount, rowCount, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fileCount", fileCount)
                .add("rowCount", rowCount)
                .add("inMemoryDataSizeInBytes", inMemoryDataSizeInBytes)
                .add("onDiskDataSizeInBytes", onDiskDataSizeInBytes)
                .toString();
    }
}
