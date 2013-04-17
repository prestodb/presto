package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
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
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.parseHiveBoolean;
import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;

class GenericHiveRecordCursor<K, V extends Writable>
        implements RecordCursor
{
    private final int partitionKeyCount;

    private final RecordReader<K, V> recordReader;
    private final K key;
    private final V value;
    private final Deserializer deserializer;

    private final ColumnType[] types;
    private final HiveType[] hiveTypes;

    private final StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;

    private final int[] hiveColumnIndexes;

    private final long[] longs;
    private final double[] doubles;
    private final byte[][] strings;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;

    public GenericHiveRecordCursor(RecordReader<K, V> recordReader, long totalBytes, Properties chunkSchema, List<HivePartitionKey> partitionKeys, List<HiveColumnHandle> columns)
    {
        Preconditions.checkNotNull(recordReader, "recordReader is null");
        Preconditions.checkArgument(totalBytes >= 0, "totalBytes is negative");
        Preconditions.checkNotNull(chunkSchema, "chunkSchema is null");
        Preconditions.checkNotNull(partitionKeys, "partitionKeys is null");
        Preconditions.checkNotNull(columns, "columns is null");
        Preconditions.checkArgument(!columns.isEmpty(), "columns is empty");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();

        try {
            this.deserializer = MetaStoreUtils.getDeserializer(null, chunkSchema);
            this.rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        this.partitionKeyCount = partitionKeys.size();
        int size = partitionKeyCount + Ordering.natural().max(Iterables.transform(columns, hiveColumnIndexGetter())) + 1;

        this.types = new ColumnType[size];
        this.hiveTypes = new HiveType[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.strings = new byte[size][];
        this.nulls = new boolean[size];

        // add partition columns first
        for (int columnIndex = 0; columnIndex < partitionKeyCount; columnIndex++) {
            HivePartitionKey partitionKey = partitionKeys.get(columnIndex);
            this.types[columnIndex] = partitionKey.getHiveType().getNativeType();
            byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);
            parseColumn(columnIndex, bytes, 0, bytes.length);
        }

        // then add data columns
        this.hiveColumnIndexes = new int[columns.size()];
        this.fieldInspectors = new ObjectInspector[columns.size()];
        this.structFields = new StructField[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);
            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            this.types[partitionKeyCount + column.getHiveColumnIndex()] = column.getHiveType().getNativeType();
            this.hiveTypes[partitionKeyCount + column.getHiveColumnIndex()] = column.getHiveType();

            StructField field = rowInspector.getStructFieldRef(column.getName());
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
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

        for (int i = 0; i < hiveColumnIndexes.length; i++) {
            int hiveColumnIndex = hiveColumnIndexes[i];
            Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
            int column = partitionKeyCount + hiveColumnIndex;

            if (fieldData == null) {
                nulls[column] = true;
            }
            else if (hiveTypes[column] == HiveType.MAP || hiveTypes[column] == HiveType.LIST || hiveTypes[column] == HiveType.STRUCT) {
                // temporarily special case MAP, LIST, and STRUCT types as strings
                strings[column] = SerDeUtils.getJSONString(fieldData, fieldInspectors[i]).getBytes(Charsets.UTF_8);
            }
            else {
                Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[i]).getPrimitiveJavaObject(fieldData);
                checkState(fieldValue != null, "fieldValue should not be null");
                switch (types[column]) {
                    case LONG:
                        longs[column] = getLongOrBoolean(fieldValue);
                        break;
                    case DOUBLE:
                        doubles[column] = ((Number) fieldValue).doubleValue();
                        break;
                    case STRING:
                        strings[column] = ((String) fieldValue).getBytes(Charsets.UTF_8);
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
