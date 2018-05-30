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
package com.facebook.presto.hive.parquet;

import io.airlift.slice.Slice;
import parquet.column.statistics.Statistics;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ParquetDataPageV2
        extends ParquetDataPage
{
    private final int rowCount;
    private final int nullCount;
    private final Slice repetitionLevels;
    private final Slice definitionLevels;
    private final ParquetEncoding dataEncoding;
    private final Slice slice;
    private final Statistics<?> statistics;
    private final boolean isCompressed;

    public ParquetDataPageV2(
            int rowCount,
            int nullCount,
            int valueCount,
            Slice repetitionLevels,
            Slice definitionLevels,
            ParquetEncoding dataEncoding,
            Slice slice,
            int uncompressedSize,
            Statistics<?> statistics,
            boolean isCompressed)
    {
        super(repetitionLevels.length() + definitionLevels.length() + slice.length(), uncompressedSize, valueCount);
        this.rowCount = rowCount;
        this.nullCount = nullCount;
        this.repetitionLevels = requireNonNull(repetitionLevels, "repetitionLevels slice is null");
        this.definitionLevels = requireNonNull(definitionLevels, "definitionLevels slice is null");
        this.dataEncoding = dataEncoding;
        this.slice = requireNonNull(slice, "slice is null");
        this.statistics = statistics;
        this.isCompressed = isCompressed;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public int getNullCount()
    {
        return nullCount;
    }

    public Slice getRepetitionLevels()
    {
        return repetitionLevels;
    }

    public Slice getDefinitionLevels()
    {
        return definitionLevels;
    }

    public ParquetEncoding getDataEncoding()
    {
        return dataEncoding;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public Statistics<?> getStatistics()
    {
        return statistics;
    }

    public boolean isCompressed()
    {
        return isCompressed;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("nullCount", nullCount)
                .add("repetitionLevels", repetitionLevels)
                .add("definitionLevels", definitionLevels)
                .add("dataEncoding", dataEncoding)
                .add("slice", slice)
                .add("statistics", statistics)
                .add("isCompressed", isCompressed)
                .add("valueCount", valueCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
