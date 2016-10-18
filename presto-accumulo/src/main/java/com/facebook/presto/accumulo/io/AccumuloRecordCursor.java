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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.facebook.presto.accumulo.AccumuloErrorCode.IO_ERROR;
import static com.facebook.presto.accumulo.io.AccumuloPageSink.ROW_ID_COLUMN;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
    private final List<AccumuloColumnHandle> columnHandles;
    private final String[] fieldToColumnName;
    private final BatchScanner scanner;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;
    private final Text prevRowID = new Text();
    private final Text rowID = new Text();

    private long bytesRead;
    private long nanoStart;
    private long nanoEnd;
    private Entry<Key, Value> prevKV;

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

        iterator = this.scanner.iterator();
    }

    @Override
    public long getTotalBytes()
    {
        return 0L; // unknown value
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

        // In this method, we are effectively doing what the WholeRowIterator is doing w/o the extra overhead.
        // We continually read key/value pairs from Accumulo,
        // deserializing each entry using the instance of AccumuloRowSerializer we were given until we see the row ID change.

        try {
            // We start with the end! When we have no more key/value pairs to read

            // If the iterator doesn't have any more values
            if (!iterator.hasNext()) {
                // Deserialize final KV pair.
                // This accounts for the edge case when the last read KV pair was a new row and we broke out of the below loop.
                if (prevKV != null) {
                    serializer.reset();
                    serializer.deserialize(prevKV);
                    prevKV = null;
                    return true;
                }
                else {
                    // else we are super done
                    return false;
                }
            }

            // If we do have key/value pairs to process from Accumulo,
            // we reset and deserialize the previously scanned key/value pair as we have started a new row.
            // This code block does not execute the first time this method is called.
            if (prevRowID.getLength() != 0) {
                serializer.reset();
                serializer.deserialize(prevKV);
            }

            // While there are key/value pairs to read and we have not advanced to a new row
            boolean advancedToNewRow = false;
            while (iterator.hasNext() && !advancedToNewRow) {
                // Scan the key value pair and get the row ID
                Entry<Key, Value> entry = iterator.next();
                bytesRead += entry.getKey().getSize() + entry.getValue().getSize();
                entry.getKey().getRow(rowID);

                // If the row IDs are equivalent or there is no previous row
                // ID (length == 0), deserialize the KV pair
                if (rowID.equals(prevRowID) || prevRowID.getLength() == 0) {
                    serializer.deserialize(entry);
                }
                else {
                    // If they are different, then we have advanced to a new row and we need to
                    // break out of the loop
                    advancedToNewRow = true;
                }

                // Set our 'previous' member variables to track the previously scanned entry
                prevKV = entry;
                prevRowID.set(rowID);
            }

            // If we didn't break out of the loop, we have reached the last entry in our
            // BatchScanner, so we set this to null as the entire row has been processed
            // This tracks the edge case where it is one entry per row ID
            if (!advancedToNewRow) {
                prevKV = null;
            }

            // Return true so Presto will process the row we've just ran
            return true;
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
            return serializer.getDate(fieldToColumnName[field]).getTime();
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
     * @return True if scanner should retriev eonly row IDs, false otherwise
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
