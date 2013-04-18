package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.parseHiveBoolean;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;

class GenericHiveRecordCursor<K, V extends Writable>
        implements RecordCursor
{
    private final RecordReader<K, V> recordReader;
    private final K key;
    private final V value;
    private final Deserializer deserializer;

    private final String[] names;
    private final ColumnType[] types;
    private final HiveType[] hiveTypes;

    private final StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;

    private final boolean[] isPartitionColumn;

    private final long[] longs;
    private final double[] doubles;
    private final byte[][] strings;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;

    public GenericHiveRecordCursor(RecordReader<K, V> recordReader, long totalBytes, Properties splitSchema, List<HivePartitionKey> partitionKeys, List<HiveColumnHandle> columns)
    {
        Preconditions.checkNotNull(recordReader, "recordReader is null");
        Preconditions.checkArgument(totalBytes >= 0, "totalBytes is negative");
        Preconditions.checkNotNull(splitSchema, "splitSchema is null");
        Preconditions.checkNotNull(partitionKeys, "partitionKeys is null");
        Preconditions.checkNotNull(columns, "columns is null");
        Preconditions.checkArgument(!columns.isEmpty(), "columns is empty");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();

        try {
            this.deserializer = MetaStoreUtils.getDeserializer(null, splitSchema);
            this.rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        int size = columns.size();

        this.names = new String[size];
        this.types = new ColumnType[size];
        this.hiveTypes = new HiveType[size];

        this.structFields = new StructField[size];
        this.fieldInspectors = new ObjectInspector[size];

        this.isPartitionColumn = new boolean[size];

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
        Map<String,HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                Preconditions.checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                parseColumn(columnIndex, bytes, 0, bytes.length);
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
    public boolean advanceNextPosition()
    {
        try {
            if (!recordReader.next(key, value)) {
                return false;
            }

            parseRecord();

            return true;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private void parseRecord()
            throws Exception
    {
        Arrays.fill(nulls, false);

        Object rowData = deserializer.deserialize(value);

        for (int outputColumnIndex = 0; outputColumnIndex < types.length; outputColumnIndex++) {
            if (isPartitionColumn[outputColumnIndex]) {
                // skip partition keys
                continue;
            }

            Object fieldData = rowInspector.getStructFieldData(rowData, structFields[outputColumnIndex]);

            if (fieldData == null) {
                nulls[outputColumnIndex] = true;
            }
            else if (hiveTypes[outputColumnIndex] == HiveType.MAP || hiveTypes[outputColumnIndex] == HiveType.LIST || hiveTypes[outputColumnIndex] == HiveType.STRUCT) {
                // temporarily special case MAP, LIST, and STRUCT types as strings
                strings[outputColumnIndex] = SerDeUtils.getJSONString(fieldData, fieldInspectors[outputColumnIndex]).getBytes(Charsets.UTF_8);
            }
            else {
                Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[outputColumnIndex]).getPrimitiveJavaObject(fieldData);
                checkState(fieldValue != null, "fieldValue should not be null");
                switch (types[outputColumnIndex]) {
                    case LONG:
                        longs[outputColumnIndex] = getLongOrBoolean(fieldValue);
                        break;
                    case DOUBLE:
                        doubles[outputColumnIndex] = ((Number) fieldValue).doubleValue();
                        break;
                    case STRING:
                        strings[outputColumnIndex] = ((String) fieldValue).getBytes(Charsets.UTF_8);
                        break;
                }
            }
        }
    }

    private static long getLongOrBoolean(Object value)
    {
        if (value instanceof Boolean) {
            return ((boolean) value) ? 1 : 0;
        }
        return ((Number) value).longValue();
    }

    private void parseColumn(int column, byte[] bytes, int start, int length)
    {
        switch (types[column]) {
            case LONG:
                if (length == 0) {
                    nulls[column] = true;
                }
                else if (hiveTypes[column] == HiveType.BOOLEAN) {
                    Boolean bool = parseHiveBoolean(bytes, start, length);
                    if (bool == null) {
                        nulls[column] = true;
                    }
                    else {
                        longs[column] = bool ? 1 : 0;
                    }
                }
                else {
                    longs[column] = parseLong(bytes, start, length);
                }
                break;
            case DOUBLE:
                if (length == 0) {
                    nulls[column] = true;
                }
                else {
                    doubles[column] = parseDouble(bytes, start, length);
                }
                break;
            case STRING:
                strings[column] = Arrays.copyOfRange(bytes, start, start + length);
                break;
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        validateType(fieldId, ColumnType.LONG);
        return longs[fieldId];
    }

    @Override
    public double getDouble(int fieldId)
    {
        validateType(fieldId, ColumnType.DOUBLE);
        return doubles[fieldId];
    }

    @Override
    public byte[] getString(int fieldId)
    {
        validateType(fieldId, ColumnType.STRING);
        return strings[fieldId];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        return nulls[fieldId];
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
        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
