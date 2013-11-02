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

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class GenericHiveRecordCursor<K, V extends Writable>
        implements RecordCursor
{
    private final RecordReader<K, V> recordReader;
    private final K key;
    private final V value;

    @SuppressWarnings("deprecation")
    private final Deserializer deserializer;

    private final ColumnType[] types;
    private final HiveType[] hiveTypes;

    private final StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;

    private final boolean[] isPartitionColumn;

    private final boolean[] loaded;
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final byte[][] strings;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;
    private Object rowData;
    private boolean closed;

    public GenericHiveRecordCursor(RecordReader<K, V> recordReader, long totalBytes, Properties splitSchema, List<HivePartitionKey> partitionKeys, List<HiveColumnHandle> columns)
    {
        checkNotNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();

        try {
            this.deserializer = MetaStoreUtils.getDeserializer(null, splitSchema);
            this.rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        }
        catch (MetaException | SerDeException | RuntimeException e) {
            throw Throwables.propagate(e);
        }

        int size = columns.size();

        String[] names = new String[size];
        this.types = new ColumnType[size];
        this.hiveTypes = new HiveType[size];

        this.structFields = new StructField[size];
        this.fieldInspectors = new ObjectInspector[size];

        this.isPartitionColumn = new boolean[size];

        this.loaded = new boolean[size];
        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.strings = new byte[size][];
        this.nulls = new boolean[size];

        // initialize data columns
        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = column.getType();
            hiveTypes[i] = column.getHiveType();

            if (!column.isPartitionKey()) {
                StructField field = rowInspector.getStructFieldRef(column.getName());
                structFields[i] = field;
                fieldInspectors[i] = field.getFieldObjectInspector();
            }

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

                switch (types[columnIndex]) {
                    case BOOLEAN:
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
                        break;
                    case LONG:
                        if (bytes.length == 0) {
                            throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                        }
                        longs[columnIndex] = parseLong(bytes, 0, bytes.length);
                        break;
                    case DOUBLE:
                        if (bytes.length == 0) {
                            throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                        }
                        doubles[columnIndex] = parseDouble(bytes, 0, bytes.length);
                        break;
                    case STRING:
                        strings[columnIndex] = Arrays.copyOf(bytes, bytes.length);
                        break;
                    default:
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
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
        return completedBytes;
    }

    @Override
    public ColumnType getType(int field)
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

            // decode value
            rowData = deserializer.deserialize(value);

            return true;
        }
        catch (IOException | SerDeException | RuntimeException e) {
            close();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, ColumnType.BOOLEAN);
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

        Object fieldData = rowInspector.getStructFieldData(rowData, structFields[column]);

        if (fieldData == null) {
            nulls[column] = true;
        }
        else {
            Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[column]).getPrimitiveJavaObject(fieldData);
            checkState(fieldValue != null, "fieldValue should not be null");
            booleans[column] = (Boolean) fieldValue;
            nulls[column] = false;
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, ColumnType.LONG);
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

        Object fieldData = rowInspector.getStructFieldData(rowData, structFields[column]);

        if (fieldData == null) {
            nulls[column] = true;
        }
        else {
            Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[column]).getPrimitiveJavaObject(fieldData);
            checkState(fieldValue != null, "fieldValue should not be null");
            longs[column] = getLongOrTimestamp(fieldValue);
            nulls[column] = false;
        }
    }

    private static long getLongOrTimestamp(Object value)
    {
        if (value instanceof Timestamp) {
            return MILLISECONDS.toSeconds(((Timestamp) value).getTime());
        }
        return ((Number) value).longValue();
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, ColumnType.DOUBLE);
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

        Object fieldData = rowInspector.getStructFieldData(rowData, structFields[column]);

        if (fieldData == null) {
            nulls[column] = true;
        }
        else {
            Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[column]).getPrimitiveJavaObject(fieldData);
            checkState(fieldValue != null, "fieldValue should not be null");
            doubles[column] = ((Number) fieldValue).doubleValue();
            nulls[column] = false;
        }
    }

    @Override
    public byte[] getString(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, ColumnType.STRING);
        if (!loaded[fieldId]) {
            parseStringColumn(fieldId);
        }
        return strings[fieldId];
    }

    private void parseStringColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPartitionColumn[column], "Column is a partition key");

        loaded[column] = true;

        Object fieldData = rowInspector.getStructFieldData(rowData, structFields[column]);

        if (fieldData == null) {
            nulls[column] = true;
        }
        else if (hiveTypes[column] == HiveType.MAP || hiveTypes[column] == HiveType.LIST || hiveTypes[column] == HiveType.STRUCT) {
            // temporarily special case MAP, LIST, and STRUCT types as strings
            strings[column] = SerDeUtils.getJSONString(fieldData, fieldInspectors[column]).getBytes(Charsets.UTF_8);
            nulls[column] = false;
        }
        else {
            Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[column]).getPrimitiveJavaObject(fieldData);
            checkState(fieldValue != null, "fieldValue should not be null");
            if (fieldValue instanceof String) {
                strings[column] = ((String) fieldValue).getBytes(Charsets.UTF_8);
            }
            else if (fieldValue instanceof byte[]) {
                strings[column] = (byte[]) fieldValue;
            }
            else {
                throw new IllegalStateException("unsupported string field type: " + fieldValue.getClass().getName());
            }
            nulls[column] = false;
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
        switch (types[column]) {
            case BOOLEAN:
                parseBooleanColumn(column);
                break;
            case LONG:
                parseLongColumn(column);
                break;
            case DOUBLE:
                parseDoubleColumn(column);
                break;
            case STRING:
                parseStringColumn(column);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + types[column]);
        }
    }

    private void validateType(int fieldId, ColumnType type)
    {
        if (types[fieldId] != type) {
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

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
