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

import com.facebook.presto.orc.BooleanVector;
import com.facebook.presto.orc.DoubleVector;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.raptor.metadata.ColumnStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
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

    public static Optional<ColumnStats> computeColumnStats(OrcReader orcReader, long columnId, Type type)
        throws IOException
    {
        return Optional.ofNullable(doComputeColumnStats(orcReader, columnId, type));
    }

    private static ColumnStats doComputeColumnStats(OrcReader orcReader, long columnId, Type type)
            throws IOException
    {
        int columnIndex = columnIndex(orcReader.getColumnNames(), columnId);
        OrcRecordReader reader = orcReader.createRecordReader(ImmutableMap.of(columnIndex, type), OrcPredicate.TRUE, UTC);

        if (type.equals(BooleanType.BOOLEAN)) {
            return indexBoolean(reader, columnIndex, columnId);
        }
        if (type.equals(BigintType.BIGINT) ||
                type.equals(DateType.DATE) ||
                type.equals(TimestampType.TIMESTAMP)) {
            return indexLong(reader, columnIndex, columnId);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return indexDouble(reader, columnIndex, columnId);
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return indexString(reader, columnIndex, columnId);
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

    private static ColumnStats indexBoolean(OrcRecordReader reader, int columnIndex, long columnId)
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
            BooleanVector vector = new BooleanVector(batchSize);
            reader.readVector(columnIndex, vector);

            for (int i = 0; i < batchSize; i++) {
                if (vector.isNull[i]) {
                    continue;
                }
                if (!minSet || Boolean.compare(vector.vector[i], min) < 0) {
                    minSet = true;
                    min = vector.vector[i];
                }
                if (!maxSet || Boolean.compare(vector.vector[i], max) > 0) {
                    maxSet = true;
                    max = vector.vector[i];
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexLong(OrcRecordReader reader, int columnIndex, long columnId)
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
            LongVector vector = new LongVector(batchSize);
            reader.readVector(columnIndex, vector);

            for (int i = 0; i < batchSize; i++) {
                if (vector.isNull[i]) {
                    continue;
                }
                if (!minSet || (vector.vector[i] < min)) {
                    minSet = true;
                    min = vector.vector[i];
                }
                if (!maxSet || (vector.vector[i] > max)) {
                    maxSet = true;
                    max = vector.vector[i];
                }
            }
        }

        return new ColumnStats(columnId,
                minSet ? min : null,
                maxSet ? max : null);
    }

    private static ColumnStats indexDouble(OrcRecordReader reader, int columnIndex, long columnId)
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
            DoubleVector vector = new DoubleVector(batchSize);
            reader.readVector(columnIndex, vector);

            for (int i = 0; i < batchSize; i++) {
                if (vector.isNull[i]) {
                    continue;
                }
                double value = vector.vector[i];
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

    private static ColumnStats indexString(OrcRecordReader reader, int columnIndex, long columnId)
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
            SliceVector vector = new SliceVector(batchSize);
            reader.readVector(columnIndex, vector);

            for (int i = 0; i < batchSize; i++) {
                Slice slice = vector.vector[i];
                if (slice == null) {
                    continue;
                }
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
