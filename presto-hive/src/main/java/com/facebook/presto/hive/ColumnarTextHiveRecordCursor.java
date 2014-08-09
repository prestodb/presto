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

import com.facebook.presto.hive.shaded.org.apache.commons.codec.binary.Base64;
import com.facebook.presto.hive.util.SerDeUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import static java.lang.Math.max;
import static java.lang.Math.min;

class ColumnarTextHiveRecordCursor<K>
        extends HiveRecordCursor
{
    private static final DateTimeFormatter DATE_PARSER = ISODateTimeFormat.date().withZone(DateTimeZone.UTC);
    private final RecordReader<K, BytesRefArrayWritable> recordReader;
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
    private final DateTimeZone hiveStorageTimeZone;
    private final DateTimeZone sessionTimeZone;

    private long completedBytes;
    private boolean closed;

    public ColumnarTextHiveRecordCursor(
            RecordReader<K, BytesRefArrayWritable> recordReader,
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
        checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();
        this.hiveStorageTimeZone = hiveStorageTimeZone;
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

        validateType(fieldId, boolean.class);
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
        boolean wasNull;
        if (isTrue(bytes, start, length)) {
            booleans[column] = true;
            wasNull = false;
        }
        else if (isFalse(bytes, start, length)) {
            booleans[column] = false;
            wasNull = false;
        }
        else {
            wasNull = true;
        }
        nulls[column] = wasNull;
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
        boolean wasNull;
        if (length == 0 || (length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N')) {
            wasNull = true;
        }
        else if (hiveTypes[column] == HiveType.DATE) {
            String value = new String(bytes, start, length);
            longs[column] = DATE_PARSER.parseMillis(value);
            wasNull = false;
        }
        else if (hiveTypes[column] == HiveType.TIMESTAMP) {
            String value = new String(bytes, start, length);
            longs[column] = parseHiveTimestamp(value, hiveStorageTimeZone);
            wasNull = false;
        }
        else {
            longs[column] = parseLong(bytes, start, length);
            wasNull = false;
        }
        nulls[column] = wasNull;
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
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
        boolean wasNull;
        if (length == 0 || (length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N')) {
            wasNull = true;
        }
        else {
            doubles[column] = parseDouble(bytes, start, length);
            wasNull = false;
        }
        nulls[column] = wasNull;
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
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
        boolean wasNull;
        if (length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N') {
            wasNull = true;
        }
        else if (hiveTypes[column] == HiveType.MAP || hiveTypes[column] == HiveType.LIST || hiveTypes[column] == HiveType.STRUCT) {
            // temporarily special case MAP, LIST, and STRUCT types as strings
            // TODO: create a real parser for these complex types when we implement data types
            LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(fieldInspectors[column]);
            ByteArrayRef byteArrayRef = new ByteArrayRef();
            byteArrayRef.setData(bytes);
            lazyObject.init(byteArrayRef, start, length);
            slices[column] = Slices.wrappedBuffer(SerDeUtils.getJsonBytes(sessionTimeZone, lazyObject.getObject(), fieldInspectors[column]));
            wasNull = false;
        }
        else {
            slices[column] = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));

            // this is unbelievably stupid but Hive base64 encodes binary data in a binary file format
            if (hiveTypes[column] == HiveType.BINARY) {
                // and yes we end up with an extra copy here because the Base64 only handles whole arrays
                slices[column] = Slices.wrappedBuffer(Base64.decodeBase64(slices[column].getBytes()));
            }
            wasNull = false;
        }
        nulls[column] = wasNull;
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
        if (type.equals(BOOLEAN)) {
            parseBooleanColumn(column);
        }
        else if (type.equals(BIGINT)) {
            parseLongColumn(column);
        }
        else if (type.equals(DOUBLE)) {
            parseDoubleColumn(column);
        }
        else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
            parseStringColumn(column);
        }
        else if (type.equals(DATE)) {
            parseLongColumn(column);
        }
        else if (type.equals(TIMESTAMP)) {
            parseLongColumn(column);
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private void validateType(int fieldId, Class<?> type)
    {
        if (!types[fieldId].getJavaType().equals(type)) {
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
