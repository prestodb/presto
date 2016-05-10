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
import com.facebook.presto.accumulo.iterators.AndFilter;
import com.facebook.presto.accumulo.iterators.NullRowFilter;
import com.facebook.presto.accumulo.iterators.OrFilter;
import com.facebook.presto.accumulo.iterators.SingleColumnValueFilter;
import com.facebook.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.StandardTypes;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.IO_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of Presto RecordCursor, responsible for iterating over a Presto split, reading
 * rows of data and then implementing various methods to retrieve columns within each row.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordSetProvider
 */
public class AccumuloRecordCursor
        implements RecordCursor
{
    private final List<AccumuloColumnHandle> cHandles;
    private final String[] fieldToColumnName;

    private long bytesRead = 0L;
    private long nanoStart = 0L;
    private long nanoEnd = 0L;
    private final BatchScanner scan;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;
    private Entry<Key, Value> prevKV;
    private Text prevRowID = new Text();
    private Text rowID = new Text();

    /**
     * Creates a new instance of {@link AccumuloRecordCursor}
     *
     * @param serializer Serializer to decode the data stored in Accumulo
     * @param scan BatchScanner for retrieving rows of data
     * @param rowIdName Presto column that is the Accumulo row ID
     * @param cHandles List of column handles in each row
     * @param constraints List of all column constraints
     */
    public AccumuloRecordCursor(AccumuloRowSerializer serializer, BatchScanner scan,
            String rowIdName, List<AccumuloColumnHandle> cHandles,
            List<AccumuloColumnConstraint> constraints)
    {
        this.cHandles = requireNonNull(cHandles, "cHandles is null");
        this.scan = requireNonNull(scan, "scan is null");

        this.serializer = requireNonNull(serializer, "serializer is null");
        this.serializer.setRowIdName(rowIdName);

        if (retrieveOnlyRowIds(rowIdName)) {
            this.scan.addScanIterator(
                    new IteratorSetting(1, "firstentryiter", FirstEntryInRowIterator.class));

            fieldToColumnName = new String[1];
            fieldToColumnName[0] = rowIdName;

            // Set a flag on the serializer saying we are only going to be retrieving the row ID
            this.serializer.setRowOnly(true);
        }
        else {
            // Else, we will be scanning some more columns here
            this.serializer.setRowOnly(false);

            Text fam = new Text();
            Text qual = new Text();

            // Create an array which maps the column ordinal to the name of the column
            fieldToColumnName = new String[cHandles.size()];
            for (int i = 0; i < cHandles.size(); ++i) {
                AccumuloColumnHandle cHandle = cHandles.get(i);
                fieldToColumnName[i] = cHandle.getName();

                // Make sure to skip the row ID!
                if (!cHandle.getName().equals(rowIdName)) {
                    // Set the mapping of presto column name to the family/qualifier
                    this.serializer.setMapping(cHandle.getName(), cHandle.getFamily(),
                            cHandle.getQualifier());

                    // Set our scanner to fetch this family/qualifier column
                    // This will help us prune which data we receive from Accumulo
                    fam.set(cHandle.getFamily());
                    qual.set(cHandle.getQualifier());
                    this.scan.fetchColumn(fam, qual);
                }
            }
        }

        // Add column iterators and retrieve the iterator
        addColumnIterators(constraints);
        iterator = this.scan.iterator();
    }

    /**
     * Gets the total bytes that need to be scanned
     *
     * @return Total bytes
     */
    @Override
    public long getTotalBytes()
    {
        return 0L; // unknown value
    }

    /**
     * Gets the bytes that have been processed so far
     *
     * @return Completed bytes
     */
    @Override
    public long getCompletedBytes()
    {
        return bytesRead;
    }

    /**
     * Get the amount of time spent reading data, in nanoseconds
     *
     * @return Read time nanos
     */
    @Override
    public long getReadTimeNanos()
    {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    /**
     * Gets the Presto type for the given field ordinal
     *
     * @param field Ordinal of the field aka column
     * @return Presto type for this field
     */
    @Override
    public Type getType(int field)
    {
        checkArgument(field < cHandles.size(), "Invalid field index");
        return cHandles.get(field).getType();
    }

    /**
     * Advances the iterator to the next Presto row
     *
     * @return True if a row was read, false otherwise
     */
    @Override
    public boolean advanceNextPosition()
    {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }

        // In this method, we are effectively doing what the WholeRowIterator is doing w/o the extra
        // overhead. We continually read key/value pairs from Accumulo, deserializing each entry
        // using the instance of AccumuloRowSerializer we were given until we see the row ID change.

        try {
            // We start with the end! When we have no more key/value pairs to read

            // If the iterator doesn't have any more values
            if (!iterator.hasNext()) {
                // Deserialize final KV pair
                // This accounts for the edge case when the last read KV pair
                // was a new row and we broke out of the below loop
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

            // If we do have key/value pairs to process from Accumulo, we reset and deserialize the
            // previously scanned key/value pair as we have started a new row.
            // This code block does not execute the first time this method is called
            if (prevRowID.getLength() != 0) {
                serializer.reset();
                serializer.deserialize(prevKV);
            }

            // While there are key/value pairs to read and we have not advanced to a new row
            boolean advancedToNewRow = false;
            while (iterator.hasNext() && !advancedToNewRow) {
                // Scan the key value pair and get the row ID
                Entry<Key, Value> kv = iterator.next();

                bytesRead += kv.getKey().getSize() + kv.getValue().getSize();
                kv.getKey().getRow(rowID);

                // If the row IDs are equivalent or there is no previous row
                // ID (length == 0), deserialize the KV pair
                if (rowID.equals(prevRowID) || prevRowID.getLength() == 0) {
                    serializer.deserialize(kv);
                }
                else {
                    // If they are different, then we have advanced to a new row and we need to
                    // break out of the loop
                    advancedToNewRow = true;
                }

                // Set our 'previous' member variables to track the previously scanned entry
                prevKV = kv;
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

    /**
     * Gets a Boolean value indicating if the field is null
     *
     * @param field Ordinal of the field
     * @return True if null, false otherwise
     */
    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < cHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    /**
     * Gets a Boolean value from the given field ordinal
     *
     * @param field Ordinal of the field
     * @return The value of the field
     */
    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return serializer.getBoolean(fieldToColumnName[field]);
    }

    /**
     * Gets a Double value from the given field ordinal
     *
     * @param field Ordinal of the field
     * @return The value of the field
     */
    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return serializer.getDouble(fieldToColumnName[field]);
    }

    /**
     * Gets a Long value from the given field ordinal
     *
     * @param field Ordinal of the field
     * @return The value of the field
     */
    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, DATE, INTEGER, TIME, TIMESTAMP);
        switch (getType(field).getDisplayName()) {
            case StandardTypes.BIGINT:
                return serializer.getLong(fieldToColumnName[field]);
            case StandardTypes.DATE:
                return serializer.getDate(fieldToColumnName[field]).getTime();
            case StandardTypes.INTEGER:
                return serializer.getInt(fieldToColumnName[field]);
            case StandardTypes.TIME:
                return serializer.getTime(fieldToColumnName[field]).getTime();
            case StandardTypes.TIMESTAMP:
                return serializer.getTimestamp(fieldToColumnName[field]).getTime();
            default:
                throw new PrestoException(INTERNAL_ERROR,
                        "Unsupported type " + getType(field));
        }
    }

    /**
     * Gets an Object value from the given field ordinal. This is called for array and map types.
     *
     * @param field Ordinal of the field
     * @return The value of the field
     */
    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type),
                "Expected field %s to be a type of array or map but is %s", field, type);

        if (Types.isArrayType(type)) {
            return serializer.getArray(fieldToColumnName[field], type);
        }

        return serializer.getMap(fieldToColumnName[field], type);
    }

    /**
     * Gets a Slice value from the given field ordinal, which is a VARCHAR or VARBINARY type.
     *
     * @param field Ordinal of the field
     * @return The value of the field
     */
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
            throw new PrestoException(INTERNAL_ERROR,
                    "Unsupported type " + type);
        }
    }

    /**
     * Closes the RecordCursor
     */
    @Override
    public void close()
    {
        scan.close();
        nanoEnd = System.nanoTime();
    }

    /**
     * Gets a Boolean value indicating whether or not the scanner should only return row IDs
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
        return cHandles.size() == 0 || (cHandles.size() == 1 && cHandles.get(0).getName().equals(rowIdName));
    }

    /**
     * Checks that the given field is one of the provided types
     *
     * @param field Ordinal of the field
     * @param expected An array of expected types
     * @throws IllegalArgumentException If the given field does not match one of the types
     */
    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type t : expected) {
            if (actual.equals(t)) {
                return;
            }
        }

        checkArgument(false, "Expected field %s to be a type of %s but is %s", field,
                StringUtils.join(expected, ","), actual);
    }

    /**
     * Configures the custom column iterators to the batch scanner based on the predicate pushdown
     * constraints.
     *
     * @param constraints A list of all column constraints to configure the iterators
     */
    private void addColumnIterators(List<AccumuloColumnConstraint> constraints)
    {
        // This function goes through all column constraints to build a kind of tree, with the nodes
        // of the tree. The leaves of the tree are either NullRowFilters, or
        // SingleColumnValueFilters, and the joints of the tree are either OrFilters or AndFilters.

        // In this loop, we process each column's constraints individually to get a list of all
        // settings, which are finally AND'd together to be the intersection of all the column
        // predicates
        AtomicInteger priority = new AtomicInteger(1);
        List<IteratorSetting> allSettings = new ArrayList<>();
        for (AccumuloColumnConstraint col : constraints) {
            // If this column's predicate domain allows null values, then add a NullRowFilter
            // setting
            Domain dom = col.getDomain();
            List<IteratorSetting> colSettings = new ArrayList<>();
            if (dom.isNullAllowed()) {
                colSettings.add(getNullFilterSetting(col, priority));
            }

            // Map types are discrete values
            if (Types.isMapType(dom.getType())) {
                // Create an iterator setting for each discrete value
                for (Object o : dom.getValues().getDiscreteValues().getValues()) {
                    IteratorSetting cfg = getIteratorSetting(priority, col, CompareOp.EQUAL,
                            dom.getType(), (Block) o);
                    if (cfg != null) {
                        colSettings.add(cfg);
                    }
                }
            }
            else {
                // For non-map types (or so it seems), all ranges are ordered
                for (Range r : dom.getValues().getRanges().getOrderedRanges()) {
                    // Create a filter setting for each range in this domain
                    IteratorSetting cfg = getFilterSettingFromRange(col, r, priority);
                    if (cfg != null) {
                        colSettings.add(cfg);
                    }
                }
            }

            // If there was only one setting, then just add it to all of our settings
            if (colSettings.size() == 1) {
                allSettings.add(colSettings.get(0));
            }
            else if (colSettings.size() > 0) {
                // If we have any iterator for this column, or them together and add it to our list
                IteratorSetting ore = OrFilter.orFilters(priority.getAndIncrement(),
                        colSettings.toArray(new IteratorSetting[0]));
                allSettings.add(ore);
            } // else there are no settings and we can move on
        }

        // If there is only one iterator, then just use that
        if (allSettings.size() == 1) {
            this.scan.addScanIterator(allSettings.get(0));
        }
        else if (allSettings.size() > 0) {
            // Else, and all settings together and add those
            this.scan.addScanIterator(AndFilter.andFilters(1, allSettings));
        }
    }

    /**
     * Gets settings for a NullRowFilter based on the column constraint
     *
     * @param col Column constraint
     * @param priority Priority of this setting, which is arbitrary in the long run since only one
     * iterator is set.
     * @return Iterator settings
     */
    private IteratorSetting getNullFilterSetting(AccumuloColumnConstraint col,
            AtomicInteger priority)
    {
        String name = String.format("%s:%d", col.getName(), priority.get());
        return new IteratorSetting(priority.getAndIncrement(), name, NullRowFilter.class,
                NullRowFilter.getProperties(col.getFamily(), col.getQualifier()));
    }

    /**
     * Gets settings for a SingleColumnValueFilter based on the given column constraint and range.
     *
     * @param col Column constraint
     * @param r Presto range to retrieve values for the column filter
     * @param priority Priority of this setting, which is arbitrary in the long run since only one
     * iterator is set.
     * @return Iterator setting, or null if the Range is all values
     */
    private IteratorSetting getFilterSettingFromRange(AccumuloColumnConstraint col, Range r,
            AtomicInteger priority)
    {
        if (r.isAll()) {
            // [min, max]
            return null;
        }
        else if (r.isSingleValue()) {
            // value = value
            return getIteratorSetting(priority, col, CompareOp.EQUAL, r.getType(),
                    r.getSingleValue());
        }
        else {
            if (r.getLow().isLowerUnbounded()) {
                // (min, x] WHERE x < 10
                CompareOp op = r.getHigh().getBound() == Bound.EXACTLY ? CompareOp.LESS_OR_EQUAL
                        : CompareOp.LESS;
                return getIteratorSetting(priority, col, op, r.getType(), r.getHigh().getValue());
            }
            else if (r.getHigh().isUpperUnbounded()) {
                // [(x, max] WHERE x > 10
                CompareOp op = r.getLow().getBound() == Bound.EXACTLY ? CompareOp.GREATER_OR_EQUAL
                        : CompareOp.GREATER;
                return getIteratorSetting(priority, col, op, r.getType(), r.getLow().getValue());
            }
            else {
                // WHERE x > 10 AND x < 20
                CompareOp op = r.getHigh().getBound() == Bound.EXACTLY ? CompareOp.LESS_OR_EQUAL
                        : CompareOp.LESS;

                IteratorSetting high =
                        getIteratorSetting(priority, col, op, r.getType(), r.getHigh().getValue());

                op = r.getLow().getBound() == Bound.EXACTLY ? CompareOp.GREATER_OR_EQUAL
                        : CompareOp.GREATER;

                IteratorSetting low =
                        getIteratorSetting(priority, col, op, r.getType(), r.getLow().getValue());

                return AndFilter.andFilters(priority.getAndIncrement(), high, low);
            }
        }
    }

    /**
     * Helper function to limit some code from getFilterSettingFromRange
     *
     * @param priority Priority of the iterator
     * @param col Column constraint
     * @param op Comparison operator for the SingleColumnValueFilter
     * @param type Presto type
     * @param value Value for the iterator
     * @return Fresh iterator settings
     */
    private IteratorSetting getIteratorSetting(AtomicInteger priority, AccumuloColumnConstraint col,
            CompareOp op, Type type, Object value)
    {
        String name = String.format("%s:%d", col.getName(), priority.get());
        byte[] valueBytes = serializer.encode(type, value);
        return new IteratorSetting(priority.getAndIncrement(), name, SingleColumnValueFilter.class,
                SingleColumnValueFilter.getProperties(col.getFamily(), col.getQualifier(), op,
                        valueBytes));
    }
}
