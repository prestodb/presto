package com.facebook.presto.hive;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.hive.HiveBooleanParser.parseHiveBoolean;
import static com.facebook.presto.hive.HiveColumn.indexGetter;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

class BytesHiveRecordCursor<K>
        implements RecordCursor
{
    private final int partitionKeyCount;

    private final RecordReader<K, BytesRefArrayWritable> recordReader;
    private final K key;
    private final BytesRefArrayWritable value;

    private final Type[] types;
    private final PrimitiveCategory[] hiveTypes;

    private final int[] hiveColumnIndexes;

    private final long[] longs;
    private final double[] doubles;
    private final byte[][] strings;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;

    public BytesHiveRecordCursor(RecordReader<K, BytesRefArrayWritable> recordReader, long totalBytes, List<HivePartitionKey> partitionKeys, List<HiveColumn> columns)
    {
        Preconditions.checkNotNull(recordReader, "recordReader is null");
        Preconditions.checkArgument(totalBytes >= 0, "totalBytes is negative");
        Preconditions.checkNotNull(partitionKeys, "partitionKeys is null");
        Preconditions.checkNotNull(columns, "columns is null");
        Preconditions.checkArgument(!columns.isEmpty(), "columns is empty");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();

        this.partitionKeyCount = partitionKeys.size();
        int size = partitionKeyCount + Ordering.natural().max(Iterables.transform(columns, indexGetter())) + 1;

        this.types = new Type[size];
        this.hiveTypes = new PrimitiveCategory[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.strings = new byte[size][];
        this.nulls = new boolean[size];

        // add partition columns first
        for (int columnIndex = 0; columnIndex < partitionKeyCount; columnIndex++) {
            HivePartitionKey partitionKey = partitionKeys.get(columnIndex);
            this.types[columnIndex] = partitionKey.getType();
            this.hiveTypes[columnIndex] = partitionKey.getHiveType();
            byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);
            parseColumn(columnIndex, bytes, 0, bytes.length);
        }

        // then add data columns
        this.hiveColumnIndexes = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            HiveColumn column = columns.get(i);
            hiveColumnIndexes[i] = column.getIndex();
            this.types[partitionKeyCount + column.getIndex()] = column.getType();
            this.hiveTypes[partitionKeyCount + column.getIndex()] = column.getHiveType();
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

        for (int hiveColumnIndex : hiveColumnIndexes) {
            if (hiveColumnIndex >= value.size()) {
                // this partition may contain fewer fields than what's declared in the schema
                nulls[partitionKeyCount + hiveColumnIndex] = true;
            }
            else {
                BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndex);

                byte[] bytes = fieldData.getData();
                int start = fieldData.getStart();
                int length = fieldData.getLength();

                if (length == "\\N".length() && bytes[start] == '\\' && bytes[start + 1] == 'N') {
                    nulls[partitionKeyCount + hiveColumnIndex] = true;
                }
                else {
                    parseColumn(partitionKeyCount + hiveColumnIndex, bytes, start, length);
                }
            }
        }
    }

    private void parseColumn(int column, byte[] bytes, int start, int length)
    {
        switch (types[column]) {
            case LONG:
                if (length == 0) {
                    nulls[column] = true;
                }
                else if (hiveTypes[column] == PrimitiveCategory.BOOLEAN) {
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
        validateType(fieldId, Type.LONG);
        return longs[fieldId];
    }

    @Override
    public double getDouble(int fieldId)
    {
        validateType(fieldId, Type.DOUBLE);
        return doubles[fieldId];
    }

    @Override
    public byte[] getString(int fieldId)
    {
        validateType(fieldId, Type.STRING);
        return strings[fieldId];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        return nulls[fieldId];
    }

    private void validateType(int fieldId, Type type)
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
