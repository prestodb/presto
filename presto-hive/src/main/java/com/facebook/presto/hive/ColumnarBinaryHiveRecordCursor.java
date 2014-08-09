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

import com.facebook.presto.hive.util.SerDeUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.getTableObjectInspector;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
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
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.lang.Math.max;
import static java.lang.Math.min;

class ColumnarBinaryHiveRecordCursor<K>
        extends HiveRecordCursor
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final RecordReader<K, BytesRefArrayWritable> recordReader;
    private final DateTimeZone sessionTimeZone;
    private final K key;
    private final BytesRefArrayWritable value;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;
    private final HiveType[] hiveTypes;

    private final ObjectInspector[] fieldInspectors; // DON'T USE THESE UNLESS EXTRACTION WILL BE SLOW ANYWAY

    private final int[] hiveColumnIndexes;

    private final boolean[] isPartitionColumn;

    private final boolean[] loaded;
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    private static final byte HIVE_EMPTY_STRING_BYTE = (byte) 0xbf;

    private static final int SIZE_OF_SHORT = 2;
    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_LONG = 8;

    private static final Set<HiveType> VALID_HIVE_STRING_TYPES = immutableEnumSet(HiveType.BINARY, HiveType.STRING, HiveType.MAP, HiveType.LIST, HiveType.STRUCT);

    public ColumnarBinaryHiveRecordCursor(RecordReader<K, BytesRefArrayWritable> recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            DateTimeZone sessionTimeZone)
    {
        checkNotNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();
        this.sessionTimeZone = sessionTimeZone;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];

        this.fieldInspectors = new ObjectInspector[size];

        this.hiveColumnIndexes = new int[size];

        this.isPartitionColumn = new boolean[size];

        this.loaded = new boolean[size];
        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.nulls = new boolean[size];

        // initialize data columns
        StructObjectInspector rowInspector = getTableObjectInspector(splitSchema);

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = column.getType();
            hiveTypes[i] = column.getHiveType();

            if (!column.isPartitionKey()) {
                fieldInspectors[i] = rowInspector.getStructFieldRef(column.getName()).getFieldObjectInspector();
            }

            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            isPartitionColumn[i] = column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                Type type = types[columnIndex];
                if (HiveUtil.isHiveNull(bytes)) {
                    nulls[columnIndex] = true;
                }
                else if (BOOLEAN.equals(type)) {
                    if (isTrue(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = true;
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = false;
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, names[columnIndex]));
                    }
                }
                else if (BIGINT.equals(type)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                    }
                    longs[columnIndex] = parseLong(bytes, 0, bytes.length);
                }
                else if (DOUBLE.equals(type)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                    }
                    doubles[columnIndex] = parseDouble(bytes, 0, bytes.length);
                }
                else if (VARCHAR.equals(type)) {
                    slices[columnIndex] = Slices.wrappedBuffer(bytes);
                }
                else if (DATE.equals(type)) {
                    longs[columnIndex] = ISODateTimeFormat.date().withZone(DateTimeZone.UTC).parseMillis(partitionKey.getValue());
                }
                else if (TIMESTAMP.equals(type)) {
                    longs[columnIndex] = parseHiveTimestamp(partitionKey.getValue(), hiveStorageTimeZone);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + type);
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
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (closed || !recordReader.next(key, value)) {
                close();
                return false;
            }

            // reset loaded flags
            // partition keys are already loaded, but everything else is not
            System.arraycopy(isPartitionColumn, 0, loaded, 0, isPartitionColumn.length);

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

        validateType(fieldId, BOOLEAN);
        if (!loaded[fieldId]) {
            parseBooleanColumn(fieldId);
        }
        return booleans[fieldId];
    }

    private void parseBooleanColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseBooleanColumn(column, bytes, start, length);
        }
    }

    private void parseBooleanColumn(int column, byte[] bytes, int start, int length)
    {
        if (length > 0) {
            booleans[column] = bytes[start] != 0;
            nulls[column] = false;
        }
        else {
            nulls[column] = true;
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!types[fieldId].equals(BIGINT) && !types[fieldId].equals(DATE) && !types[fieldId].equals(TIMESTAMP)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, %s or %s , actual %s (field %s)", BIGINT, DATE, TIMESTAMP, types[fieldId], fieldId));
        }
        if (!loaded[fieldId]) {
            parseLongColumn(fieldId);
        }
        return longs[fieldId];
    }

    private void parseLongColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseLongColumn(column, bytes, start, length);
        }
    }

    private void parseLongColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
            return;
        }
        nulls[column] = false;
        switch (hiveTypes[column]) {
            case SHORT:
                // the file format uses big endian
                checkState(length == SIZE_OF_SHORT, "Short should be 2 bytes");
                longs[column] = Short.reverseBytes(ByteArrays.getShort(bytes, start));
                break;
            case DATE:
                checkState(length >= 1, "Date should be at least 1 byte");
                int daysSinceEpoch = readVInt(bytes, start, length);
                longs[column] = daysSinceEpoch * MILLIS_IN_DAY;
                break;
            case TIMESTAMP:
                checkState(length >= 1, "Timestamp should be at least 1 byte");
                long seconds = TimestampWritable.getSeconds(bytes, start);
                long nanos = TimestampWritable.getNanos(bytes, start + SIZE_OF_INT);
                longs[column] = (seconds * 1000) + (nanos / 1_000_000);
                break;
            case BYTE:
                checkState(length == 1, "Byte should be 1 byte");
                longs[column] = bytes[start];
                break;
            case INT:
                checkState(length >= 1, "Int should be at least 1 byte");
                if (length == 1) {
                    longs[column] = bytes[start];
                }
                else {
                    longs[column] = readVInt(bytes, start, length);
                }
                break;
            case LONG:
                checkState(length >= 1, "Long should be at least 1 byte");
                if (length == 1) {
                    longs[column] = bytes[start];
                }
                else {
                    long value = 0;
                    for (int i = 1; i < length; i++) {
                        value <<= 8;
                        value |= (bytes[start + i] & 0xFF);
                    }
                    longs[column] = WritableUtils.isNegativeVInt(bytes[start]) ? ~value : value;
                }
                break;
            default:
                throw new RuntimeException(String.format("%s is not a valid LONG type", hiveTypes[column]));
        }
    }

    private int readVInt(byte[] bytes, int start, int length)
    {
        int value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (bytes[start + i] & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[start]) ? ~value : value;
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, DOUBLE);
        if (!loaded[fieldId]) {
            parseDoubleColumn(fieldId);
        }
        return doubles[fieldId];
    }

    private void parseDoubleColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseDoubleColumn(column, bytes, start, length);
        }
    }

    private void parseDoubleColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            switch (hiveTypes[column]) {
                case FLOAT:
                    // the file format uses big endian
                    checkState(length == SIZE_OF_INT, "Float should be 4 bytes");
                    int intBits = ByteArrays.getInt(bytes, start);
                    doubles[column] = Float.intBitsToFloat(Integer.reverseBytes(intBits));
                    break;
                case DOUBLE:
                    // the file format uses big endian
                    checkState(length == SIZE_OF_LONG, "Double should be 8 bytes");
                    long longBits = ByteArrays.getLong(bytes, start);
                    doubles[column] = Double.longBitsToDouble(Long.reverseBytes(longBits));
                    break;
                default:
                    throw new RuntimeException(String.format("%s is not a valid DOUBLE type", hiveTypes[column]));
            }
        }
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!types[fieldId].equals(VARCHAR) && !types[fieldId].equals(VARBINARY)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be VARCHAR or VARBINARY, actual %s (field %s)", types[fieldId], fieldId));
        }

        if (!loaded[fieldId]) {
            parseStringColumn(fieldId);
        }
        return slices[fieldId];
    }

    private void parseStringColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseStringColumn(column, bytes, start, length);
        }
    }

    private void parseStringColumn(int column, byte[] bytes, int start, int length)
    {
        checkState(VALID_HIVE_STRING_TYPES.contains(hiveTypes[column]), "%s is not a valid STRING type", hiveTypes[column]);
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            if (hiveTypes[column] == HiveType.MAP || hiveTypes[column] == HiveType.LIST || hiveTypes[column] == HiveType.STRUCT) {
                // temporarily special case MAP, LIST, and STRUCT types as strings
                // TODO: create a real parser for these complex types when we implement data types
                LazyBinaryObject<? extends ObjectInspector> lazyObject = LazyBinaryFactory.createLazyBinaryObject(fieldInspectors[column]);
                ByteArrayRef byteArrayRef = new ByteArrayRef();
                byteArrayRef.setData(bytes);
                lazyObject.init(byteArrayRef, start, length);
                slices[column] = Slices.wrappedBuffer(SerDeUtils.getJsonBytes(sessionTimeZone, lazyObject.getObject(), fieldInspectors[column]));
            }
            else {
                // TODO: zero length BINARY is not supported. See https://issues.apache.org/jira/browse/HIVE-2483
                if (hiveTypes[column] == HiveType.STRING && (length == 1) && bytes[start] == HIVE_EMPTY_STRING_BYTE) {
                    slices[column] = Slices.EMPTY_SLICE;
                }
                else {
                    slices[column] = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));
                }
            }
        }
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!loaded[fieldId]) {
            parseColumn(fieldId);
        }
        return nulls[fieldId];
    }

    private void parseColumn(int column)
    {
        Type type = types[column];
        if (BOOLEAN.equals(type)) {
            parseBooleanColumn(column);
        }
        else if (BIGINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (DOUBLE.equals(type)) {
            parseDoubleColumn(column);
        }
        else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
            parseStringColumn(column);
        }
        else if (DATE.equals(type)) {
            parseLongColumn(column);
        }
        else if (TIMESTAMP.equals(type)) {
            parseLongColumn(column);
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private void validateType(int fieldId, Type type)
    {
        if (!types[fieldId].equals(type)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", type, types[fieldId], fieldId));
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
