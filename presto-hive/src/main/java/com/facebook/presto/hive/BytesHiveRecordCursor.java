package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.parseHiveBoolean;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class BytesHiveRecordCursor<K>
        implements RecordCursor
{
    private final RecordReader<K, BytesRefArrayWritable> recordReader;
    private final K key;
    private final BytesRefArrayWritable value;

    private final String[] names;
    private final ColumnType[] types;
    private final HiveType[] hiveTypes;

    private final ObjectInspector[] fieldInspectors; // DON'T USE THESE UNLESS EXTRACTION WILL BE SLOW ANYWAY

    private final int[] hiveColumnIndexes;

    private final long[] longs;
    private final double[] doubles;
    private final byte[][] strings;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;

    public BytesHiveRecordCursor(RecordReader<K, BytesRefArrayWritable> recordReader, long totalBytes, Properties splitSchema, List<HivePartitionKey> partitionKeys, List<HiveColumnHandle> columns)
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

        int size = columns.size();

        this.names = new String[size];
        this.types = new ColumnType[size];
        this.hiveTypes = new HiveType[size];
        this.fieldInspectors = new ObjectInspector[size];

        this.hiveColumnIndexes = new int[size];

        this.longs = new long[size];
        this.doubles = new double[size];
        this.strings = new byte[size][];
        this.nulls = new boolean[size];

        // initialize data columns
        try {
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, splitSchema);
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);

                names[i] = column.getName();
                types[i] = column.getType();
                hiveTypes[i] = column.getHiveType();

                if (!column.isPartitionKey()) {
                    fieldInspectors[i] = rowInspector.getStructFieldRef(column.getName()).getFieldObjectInspector();
                }

                hiveColumnIndexes[i] = column.getHiveColumnIndex();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        // parse requested partition columns
        Map<String,HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                Preconditions.checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                parsePrimitiveColumn(columnIndex, bytes, 0, bytes.length);
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
            throws IOException
    {
        Arrays.fill(nulls, false);

        for (int outputColumnIndex = 0; outputColumnIndex < types.length; outputColumnIndex++) {
            if (hiveColumnIndexes[outputColumnIndex] < 0) {
                // skip partition keys
                continue;
            }

            if (hiveColumnIndexes[outputColumnIndex] >= value.size()) {
                // this partition may contain fewer fields than what's declared in the schema
                // this happens when additional columns are added to the hive table after a partition has been created
                nulls[outputColumnIndex] = true;
            }
            else {
                BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[outputColumnIndex]);

                byte[] bytes = fieldData.getData();
                int start = fieldData.getStart();
                int length = fieldData.getLength();

                if (length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N') {
                    nulls[outputColumnIndex] = true;
                }
                else if (hiveTypes[outputColumnIndex] == HiveType.MAP || hiveTypes[outputColumnIndex] == HiveType.LIST || hiveTypes[outputColumnIndex] == HiveType.STRUCT) {
                    // temporarily special case MAP, LIST, and STRUCT types as strings
                    // TODO: create a real parser for these complex types when we implement data types
                    LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(fieldInspectors[outputColumnIndex]);
                    ByteArrayRef byteArrayRef = new ByteArrayRef();
                    byteArrayRef.setData(bytes);
                    lazyObject.init(byteArrayRef, start, length);
                    strings[outputColumnIndex] = SerDeUtils.getJSONString(lazyObject.getObject(), fieldInspectors[outputColumnIndex]).getBytes(Charsets.UTF_8);
                }
                else {
                    parsePrimitiveColumn(outputColumnIndex, bytes, start, length);
                }
            }
        }
    }

    private void parsePrimitiveColumn(int column, byte[] bytes, int start, int length)
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
                else if (hiveTypes[column] == HiveType.TIMESTAMP) {
                    String value = new String(bytes, start, length);
                    longs[column] = MILLISECONDS.toSeconds(HiveUtil.HIVE_TIMESTAMP_PARSER.parseMillis(value));
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

                // this is unbelievably stupid but Hive base64 encodes binary data in a binary file format
                if (hiveTypes[column] == HiveType.BINARY) {
                    // and yes we end up with an extra copy here because the Base64 only handles whole arrays
                    strings[column] = Base64.decodeBase64(strings[column]);
                }
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
