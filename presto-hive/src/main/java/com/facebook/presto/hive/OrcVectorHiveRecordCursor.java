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
package com.facebook.presto.hive;

import com.facebook.presto.hive.orc.BooleanVector;
import com.facebook.presto.hive.orc.DoubleVector;
import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.OrcRecordReader;
import com.facebook.presto.hive.orc.SliceVector;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;

class OrcVectorHiveRecordCursor
        extends HiveRecordCursor
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final OrcRecordReader recordReader;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;
    private final HiveType[] hiveTypes;

    private final int[] hiveColumnIndexes;

    private final boolean[] isPartitionColumn;

    private int batchSize;
    private int batchIndex;

    private final boolean[] isLoaded;
    private final Vector[] columns;
    private final boolean[][] nullVectors;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    public OrcVectorHiveRecordCursor(OrcRecordReader recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone sessionTimeZone)
    {
        checkNotNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];

        this.isLoaded = new boolean[size];
        this.columns = new Vector[size];
        this.nullVectors = new boolean[size][];

        this.hiveColumnIndexes = new int[size];

        this.isPartitionColumn = new boolean[size];

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            Type type = column.getType();
            types[i] = type;
            hiveTypes[i] = column.getHiveType();

            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            isPartitionColumn[i] = column.isPartitionKey();
            isLoaded[i] = column.isPartitionKey();

            if (BOOLEAN.equals(type)) {
                this.columns[i] = new BooleanVector();
            }
            else if (BIGINT.equals(type)) {
                this.columns[i] = new LongVector();
            }
            else if (DOUBLE.equals(type)) {
                this.columns[i] = new DoubleVector();
            }
            else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
                this.columns[i] = new SliceVector();
            }
            else if (DATE.equals(type)) {
                this.columns[i] = new LongVector();
            }
            else if (TIMESTAMP.equals(type)) {
                this.columns[i] = new LongVector();
            }
            else {
                throw new UnsupportedOperationException("Unsupported column type: " + type);
            }

            if (column.isPartitionKey() || VARCHAR.equals(type) || VARBINARY.equals(type)) {
                nullVectors[i] = new boolean[Vector.MAX_VECTOR_LENGTH];
            }
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                if (types[columnIndex].equals(BOOLEAN)) {
                    BooleanVector columnVector = new BooleanVector();
                    this.columns[columnIndex] = columnVector;
                    if (isTrue(bytes, 0, bytes.length)) {
                        columnVector.fill(true);
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        columnVector.fill(false);
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, names[columnIndex]));
                    }
                }
                else if (types[columnIndex].equals(BIGINT)) {
                    LongVector columnVector = new LongVector();
                    this.columns[columnIndex] = columnVector;
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                    }
                    columnVector.fill(parseLong(bytes, 0, bytes.length));
                }
                else if (types[columnIndex].equals(DOUBLE)) {
                    DoubleVector columnVector = new DoubleVector();
                    this.columns[columnIndex] = columnVector;
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                    }
                    columnVector.fill(parseDouble(bytes, 0, bytes.length));
                }
                else if (types[columnIndex].equals(VARCHAR)) {
                    SliceVector columnVector = new SliceVector();
                    this.columns[columnIndex] = columnVector;
                    columnVector.fill(Slices.wrappedBuffer(bytes));
                }
                else if (types[columnIndex].equals(DATE)) {
                    LongVector columnVector = new LongVector();
                    this.columns[columnIndex] = columnVector;
                    long millis = ISODateTimeFormat.date().withZone(DateTimeZone.UTC).parseMillis(partitionKey.getValue());
                    long days = millis / MILLIS_IN_DAY;
                    columnVector.fill(days);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + types[columnIndex]);
                }
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    private void updateCompletedBytes()
    {
        long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
        completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        batchIndex++;
        if (batchIndex < batchSize) {
            return true;
        }

        if (closed) {
            return false;
        }

        try {
            batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return false;
            }

            batchIndex = 0;

            // reset column vectors
            System.arraycopy(isPartitionColumn, 0, isLoaded, 0, isPartitionColumn.length);

            return true;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR.toErrorCode(), e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        BooleanVector column = (BooleanVector) getVector(fieldId);
        return column.vector[batchIndex];
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        LongVector column = (LongVector) getVector(fieldId);
        long value = column.vector[batchIndex];
        if (hiveTypes[fieldId] == HiveType.DATE) {
            value *= MILLIS_IN_DAY;
        }
        return value;
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        DoubleVector column = (DoubleVector) getVector(fieldId);
        return column.vector[batchIndex];
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        SliceVector column = (SliceVector) getVector(fieldId);
        return column.slice[batchIndex];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        loadVectorIfNecessary(fieldId);
        boolean[] column = nullVectors[fieldId];
        return column[batchIndex];
    }

    private boolean[] getNullVector(int fieldId)
    {
        loadVectorIfNecessary(fieldId);
        return nullVectors[fieldId];
    }

    private Vector getVector(int fieldId)
    {
        loadVectorIfNecessary(fieldId);
        return columns[fieldId];
    }

    private void loadVectorIfNecessary(int fieldId)
    {
        if (!isLoaded[fieldId]) {
            Vector vector = columns[fieldId];
            try {
                recordReader.readVector(hiveColumnIndexes[fieldId], vector);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            Type type = types[fieldId];
            if (BOOLEAN.equals(type)) {
                nullVectors[fieldId] = ((BooleanVector) vector).isNull;
            }
            else if (BIGINT.equals(type)) {
                nullVectors[fieldId] = ((LongVector) vector).isNull;
            }
            else if (DOUBLE.equals(type)) {
                nullVectors[fieldId] = ((DoubleVector) vector).isNull;
            }
            else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
                Slice[] sliceVector = ((SliceVector) vector).slice;
                for (int i = 0; i < sliceVector.length; i++) {
                    nullVectors[fieldId][i] = sliceVector[i] == null;
                }
            }
            else if (DATE.equals(type)) {
                nullVectors[fieldId] = ((LongVector) vector).isNull;
            }
            else if (TIMESTAMP.equals(type)) {
                nullVectors[fieldId] = ((LongVector) vector).isNull;
            }
            else {
                throw new UnsupportedOperationException("Unsupported column type: " + type);
            }
            isLoaded[fieldId] = true;
        }
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
