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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.facebook.presto.accumulo.AccumuloErrorCode.IO_ERROR;
import static com.facebook.presto.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of Presto RecordCursor, responsible for iterating over a Presto split,
 * reading rows of data and then implementing various methods to retrieve columns within each row.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordSetProvider
 */
public class AccumuloRecordCursor
        implements RecordCursor
{
    private static final int WHOLE_ROW_ITERATOR_PRIORITY = Integer.MAX_VALUE;

    private final List<AccumuloColumnHandle> columnHandles;
    private final String[] fieldToColumnName;
    private final BatchScanner scanner;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;

    private long bytesRead;
    private long nanoStart;
    private long nanoEnd;

    public AccumuloRecordCursor(
            AccumuloRowSerializer serializer,
            BatchScanner scanner,
            String rowIdName,
            List<AccumuloColumnHandle> columnHandles,
            List<AccumuloColumnConstraint> constraints)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.scanner = requireNonNull(scanner, "scanner is null");
        this.serializer = requireNonNull(serializer, "serializer is null");
        this.serializer.setRowIdName(requireNonNull(rowIdName, "rowIdName is null"));

        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(constraints, "constraints is null");

        if (retrieveOnlyRowIds(rowIdName)) {
            this.scanner.addScanIterator(new IteratorSetting(1, "firstentryiter", FirstEntryInRowIterator.class));

            fieldToColumnName = new String[1];
            fieldToColumnName[0] = rowIdName;

            // Set a flag on the serializer saying we are only going to be retrieving the row ID
            this.serializer.setRowOnly(true);
        }
        else {
            // Else, we will be scanning some more columns here
            this.serializer.setRowOnly(false);

            // Fetch the reserved row ID column
            this.scanner.fetchColumn(ROW_ID_COLUMN, ROW_ID_COLUMN);

            Text family = new Text();
            Text qualifier = new Text();

            // Create an array which maps the column ordinal to the name of the column
            fieldToColumnName = new String[columnHandles.size()];
            for (int i = 0; i < columnHandles.size(); ++i) {
                AccumuloColumnHandle columnHandle = columnHandles.get(i);
                fieldToColumnName[i] = columnHandle.getName();

                // Make sure to skip the row ID!
                if (!columnHandle.getName().equals(rowIdName)) {
                    // Set the mapping of presto column name to the family/qualifier
                    this.serializer.setMapping(columnHandle.getName(), columnHandle.getFamily().get(), columnHandle.getQualifier().get());

                    // Set our scanner to fetch this family/qualifier column
                    // This will help us prune which data we receive from Accumulo
                    family.set(columnHandle.getFamily().get());
                    qualifier.set(columnHandle.getQualifier().get());
                    this.scanner.fetchColumn(family, qualifier);
                }
            }
        }

        IteratorSetting setting = new IteratorSetting(WHOLE_ROW_ITERATOR_PRIORITY, WholeRowIterator.class);
        scanner.addScanIterator(setting);

        iterator = this.scanner.iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    @Override
    public long getReadTimeNanos()
    {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field >= 0 && field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }

        try {
            if (iterator.hasNext()) {
                serializer.reset();
                Entry<Key, Value> row = iterator.next();
                for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(row.getKey(), row.getValue()).entrySet()) {
                    bytesRead += entry.getKey().getSize() + entry.getValue().getSize();
                    serializer.deserialize(entry);
                }
                return true;
            }
            else {
                return false;
            }
        }
        catch (IOException e) {
            throw new PrestoException(IO_ERROR, "Caught IO error from serializer on read", e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return serializer.getBoolean(fieldToColumnName[field]);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return serializer.getDouble(fieldToColumnName[field]);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, DATE, INTEGER, REAL, SMALLINT, TIME, TIMESTAMP, TINYINT);
        Type type = getType(field);
        if (type.equals(BIGINT)) {
            return serializer.getLong(fieldToColumnName[field]);
        }
        else if (type.equals(DATE)) {
            return MILLISECONDS.toDays(serializer.getDate(fieldToColumnName[field]).getTime());
        }
        else if (type.equals(INTEGER)) {
            return serializer.getInt(fieldToColumnName[field]);
        }
        else if (type.equals(REAL)) {
            return Float.floatToIntBits(serializer.getFloat(fieldToColumnName[field]));
        }
        else if (type.equals(SMALLINT)) {
            return serializer.getShort(fieldToColumnName[field]);
        }
        else if (type.equals(TIME)) {
            return serializer.getTime(fieldToColumnName[field]).getTime();
        }
        else if (type.equals(TIMESTAMP)) {
            return serializer.getTimestamp(fieldToColumnName[field]).getTime();
        }
        else if (type.equals(TINYINT)) {
            return serializer.getByte(fieldToColumnName[field]);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type), "Expected field %s to be a type of array or map but is %s", field, type);

        if (Types.isArrayType(type)) {
            return serializer.getArray(fieldToColumnName[field], type);
        }

        return serializer.getMap(fieldToColumnName[field], type);
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = getType(field);
        if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer(serializer.getVarbinary(fieldToColumnName[field]));
        }
        else if (type instanceof VarcharType) {
            return Slices.utf8Slice(serializer.getVarchar(fieldToColumnName[field]));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + type);
        }
    }

    @Override
    public void close()
    {
        scanner.close();
        nanoEnd = System.nanoTime();
    }

    /**
     * Gets a Boolean value indicating whether or not the scanner should only return row IDs.
     * <p>
     * This can occur in cases such as SELECT COUNT(*) or the table only has one column.
     * Presto doesn't need the entire contents of the row to count them,
     * so we can configure Accumulo to only give us the first key/value pair in the row
     *
     * @param rowIdName Row ID column name
     * @return True if scanner should retrieve only row IDs, false otherwise
     */
    private boolean retrieveOnlyRowIds(String rowIdName)
    {
        return columnHandles.isEmpty() || (columnHandles.size() == 1 && columnHandles.get(0).getName().equals(rowIdName));
    }

    /**
     * Checks that the given field is one of the provided types.
     *
     * @param field Ordinal of the field
     * @param expected An array of expected types
     * @throws IllegalArgumentException If the given field does not match one of the types
     */
    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }

        throw new IllegalArgumentException(format("Expected field %s to be a type of %s but is %s", field, StringUtils.join(expected, ","), actual));
    }
}
