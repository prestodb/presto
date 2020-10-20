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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.raptor.RaptorOrcAggregatedMemoryContext;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.OrcStorageManager.toOrcFileType;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static org.joda.time.DateTimeZone.UTC;

public final class ShardStats
{
    /**
     * Maximum length of a binary value stored in an index.
     */
    public static final int MAX_BINARY_INDEX_SIZE = 100;

    private ShardStats() {}

    public static Slice truncateIndexValue(Slice slice)
    {
        if (slice.length() > MAX_BINARY_INDEX_SIZE) {
            return slice.slice(0, MAX_BINARY_INDEX_SIZE);
        }
        return slice;
    }

    public static Optional<ColumnStats> computeColumnStats(OrcReader orcReader, long columnId, Type type, TypeManager typeManager)
            throws IOException
    {
        return Optional.ofNullable(doComputeColumnStats(orcReader, columnId, type, typeManager));
    }

    private static ColumnStats doComputeColumnStats(OrcReader orcReader, long columnId, Type type, TypeManager typeManager)
            throws IOException
    {
        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(typeManager);

        int columnIndex = columnIndex(orcReader.getColumnNames(), columnId);
        OrcBatchRecordReader reader = orcReader.createBatchRecordReader(
                storageTypeConverter.toStorageTypes(ImmutableMap.of(columnIndex, toOrcFileType(type, typeManager))),
                OrcPredicate.TRUE,
                UTC,
                new RaptorOrcAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE);

        if (type.equals(BOOLEAN)) {
            return indexBoolean(reader, columnIndex, columnId);
        }
        if (type.equals(BigintType.BIGINT) ||
                type.equals(DateType.DATE) ||
                type.equals(TimeType.TIME) ||
                type.equals(TimestampType.TIMESTAMP)) {
            return indexLong(type, reader, columnIndex, columnId);
        }
        if (type.equals(DOUBLE)) {
            return indexDouble(reader, columnIndex, columnId);
        }
        if (type instanceof VarcharType) {
            return indexString(type, reader, columnIndex, columnId);
        }
        return null;
    }

    private static int columnIndex(List<String> columnNames, long columnId)
    {
        int index = columnNames.indexOf(String.valueOf(columnId));
        if (index == -1) {
            throw new PrestoException(RAPTOR_ERROR, "Missing column ID: " + columnId);
        }
        return index;
    }

    private static ColumnStats indexBoolean(OrcBatchRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        boolean min = false;
        boolean max = false;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                boolean value = BOOLEAN.getBoolean(block, i);
                if (!minSet || Boolean.compare(value, min) < 0) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || Boolean.compare(value, max) > 0) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexLong(Type type, OrcBatchRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        long min = 0;
        long max = 0;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = type.getLong(block, i);
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexDouble(OrcBatchRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        double min = 0;
        double max = 0;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                double value = DOUBLE.getDouble(block, i);
                if (isNaN(value)) {
                    continue;
                }
                if (value == -0.0) {
                    value = 0.0;
                }
                if (!minSet || (value < min)) {
                    minSet = true;
                    min = value;
                }
                if (!maxSet || (value > max)) {
                    maxSet = true;
                    max = value;
                }
            }
        }

        if (isInfinite(min)) {
            minSet = false;
        }
        if (isInfinite(max)) {
            maxSet = false;
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexString(Type type, OrcBatchRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        boolean minSet = false;
        boolean maxSet = false;
        Slice min = null;
        Slice max = null;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                Slice slice = type.getSlice(block, i);
                slice = truncateIndexValue(slice);
                if (!minSet || (slice.compareTo(min) < 0)) {
                    minSet = true;
                    min = slice;
                }
                if (!maxSet || (slice.compareTo(max) > 0)) {
                    maxSet = true;
                    max = slice;
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min.toStringUtf8() : null,
                maxSet ? max.toStringUtf8() : null);
    }
}
