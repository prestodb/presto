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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.doubleToSortableLong;
import static com.facebook.presto.raptorx.util.FloatingPointUtil.floatToSortableInt;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcRecordReader;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class ColumnStatsBuilder
{
    /**
     * Maximum length of a binary value stored in an index.
     */
    public static final int MAX_BINARY_INDEX_SIZE = 50;

    private ColumnStatsBuilder() {}

    public static byte[] truncateIndexValue(byte[] value)
    {
        if (value.length > MAX_BINARY_INDEX_SIZE) {
            return Arrays.copyOfRange(value, 0, MAX_BINARY_INDEX_SIZE);
        }
        return value;
    }

    public static Slice truncateIndexValue(Slice slice)
    {
        if (slice.length() > MAX_BINARY_INDEX_SIZE) {
            return slice.slice(0, MAX_BINARY_INDEX_SIZE);
        }
        return slice;
    }

    public static Optional<ColumnStats> computeColumnStats(OrcReader orcReader, long columnId, Type type)
            throws IOException
    {
        return Optional.ofNullable(doComputeColumnStats(orcReader, columnId, type));
    }

    private static ColumnStats doComputeColumnStats(OrcReader orcReader, long columnId, Type type)
            throws IOException
    {
        int columnIndex = columnIndex(orcReader.getColumnNames(), columnId);
        OrcRecordReader reader = createOrcRecordReader(orcReader, ImmutableMap.of(columnIndex, type));

        if (type.equals(BOOLEAN)) {
            return indexBoolean(type, reader, columnIndex, columnId);
        }
        if (type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP)) {
            return indexLong(type, reader, columnIndex, columnId, Type::getLong);
        }
        if (type.equals(DOUBLE)) {
            return indexLong(type, reader, columnIndex, columnId, ColumnStatsBuilder::readDouble);
        }
        if (type.equals(REAL)) {
            return indexLong(type, reader, columnIndex, columnId, ColumnStatsBuilder::readReal);
        }
        if (isVarcharType(type) || type.equals(VARBINARY)) {
            return indexBinary(type, reader, columnIndex, columnId);
        }
        return null;
    }

    private static int columnIndex(List<String> columnNames, long columnId)
    {
        int index = columnNames.indexOf(String.valueOf(columnId));
        if (index == -1) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Missing column ID: " + columnId);
        }
        return index;
    }

    private static ColumnStats indexBoolean(Type type, OrcRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        boolean set = false;
        boolean min = false;
        boolean max = false;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(type, columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                boolean value = type.getBoolean(block, i);
                if (!set) {
                    set = true;
                    min = value;
                    max = value;
                }
                else if (!value) {
                    min = false;
                }
                else {
                    max = true;
                }
            }
        }

        return new ColumnStats(columnId, set ? min : null, set ? max : null);
    }

    private static ColumnStats indexLong(Type type, OrcRecordReader reader, int columnIndex, long columnId, ToLong toLong)
            throws IOException
    {
        boolean set = false;
        long min = 0;
        long max = 0;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(type, columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = toLong.apply(type, block, i);
                if (!set) {
                    set = true;
                    min = value;
                    max = value;
                }
                else if (value < min) {
                    min = value;
                }
                else if (value > max) {
                    max = value;
                }
            }
        }

        return new ColumnStats(columnId, set ? min : null, set ? max : null);
    }

    private static ColumnStats indexBinary(Type type, OrcRecordReader reader, int columnIndex, long columnId)
            throws IOException
    {
        Slice min = null;
        Slice max = null;

        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = reader.readBlock(type, columnIndex);

            for (int i = 0; i < batchSize; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                Slice slice = type.getSlice(block, i);
                slice = truncateIndexValue(slice);
                if ((min == null) || (slice.compareTo(min) < 0)) {
                    min = slice;
                }
                if ((max == null) || (slice.compareTo(max) > 0)) {
                    max = slice;
                }
            }
        }

        return new ColumnStats(columnId,
                (min != null) ? min.getBytes() : null,
                (max != null) ? max.getBytes() : null);
    }

    private static long readDouble(Type type, Block block, int position)
    {
        return doubleToSortableLong(type.getDouble(block, position));
    }

    private static long readReal(Type type, Block block, int position)
    {
        return floatToSortableInt(intBitsToFloat(toIntExact(type.getLong(block, position))));
    }

    private interface ToLong
    {
        long apply(Type type, Block block, int position);
    }
}
