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

public class ParquetDataPageV1
        extends ParquetDataPage
{
    private final Slice slice;
    private final Statistics<?> statistics;
    private final ParquetEncoding repetitionLevelEncoding;
    private final ParquetEncoding definitionLevelEncoding;
    private final ParquetEncoding valuesEncoding;

    public ParquetDataPageV1(
            Slice slice,
            int valueCount,
            int uncompressedSize,
            Statistics<?> statistics,
            ParquetEncoding repetitionLevelEncoding,
            ParquetEncoding definitionLevelEncoding,
            ParquetEncoding valuesEncoding)
    {
        super(slice.length(), uncompressedSize, valueCount);
        this.slice = requireNonNull(slice, "slice is null");
        this.statistics = statistics;
        this.repetitionLevelEncoding = repetitionLevelEncoding;
        this.definitionLevelEncoding = definitionLevelEncoding;
        this.valuesEncoding = valuesEncoding;
    }

    public Slice getSlice()
    {
        return slice;
    }

    public Statistics<?> getStatistics()
    {
        return statistics;
    }

    public ParquetEncoding getDefinitionLevelEncoding()
    {
        return definitionLevelEncoding;
    }

    public ParquetEncoding getRepetitionLevelEncoding()
    {
        return repetitionLevelEncoding;
    }

    public ParquetEncoding getValueEncoding()
    {
        return valuesEncoding;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("slice", slice)
                .add("statistics", statistics)
                .add("repetitionLevelEncoding", repetitionLevelEncoding)
                .add("definitionLevelEncoding", definitionLevelEncoding)
                .add("valuesEncoding", valuesEncoding)
                .add("valueCount", valueCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
