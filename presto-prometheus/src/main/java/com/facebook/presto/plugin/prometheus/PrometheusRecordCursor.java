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
package com.facebook.presto.plugin.prometheus;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_OUTPUT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrometheusRecordCursor
        implements RecordCursor
{
    private final List<PrometheusColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final Iterator<PrometheusStandardizedRow> metricsItr;
    private final long totalBytes;

    private PrometheusStandardizedRow fields;

    public PrometheusRecordCursor(List<PrometheusColumnHandle> columnHandles, ByteSource byteSource)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            PrometheusColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            metricsItr = prometheusResultsInStandardizedForm(new PrometheusQueryResponse(input).getResults()).iterator();
            totalBytes = input.getCount();
        }
        catch (PrestoException ex) {
            throw ex;
        }
        catch (IOException e) {
            throw new PrestoException(PROMETHEUS_OUTPUT_ERROR, "Unable to handle Prometheus result: " + e.toString());
        }
    }

    static Block getBlockFromMap(Type mapType, Map<?, ?> map)
    {
        // on functions like COUNT() the Type won't be a MapType
        if (!(mapType instanceof MapType)) {
            return null;
        }
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            writeObject(builder, keyType, entry.getKey());
            writeObject(builder, valueType, entry.getValue());
        }

        mapBlockBuilder.closeEntry();
        return (Block) mapType.getObject(mapBlockBuilder, 0);
    }

    static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        MapType mapType = (MapType) type;
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
        }
        return map;
    }

    private static void writeObject(BlockBuilder builder, Type type, Object obj)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            for (Object item : (List<?>) obj) {
                writeObject(arrayBuilder, elementType, item);
            }
            builder.closeEntry();
        }
        else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            BlockBuilder mapBlockBuilder = builder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                writeObject(mapBlockBuilder, mapType.getKeyType(), entry.getKey());
                writeObject(mapBlockBuilder, mapType.getValueType(), entry.getValue());
            }
            builder.closeEntry();
        }
        else {
            if (BOOLEAN.equals(type)
                    || TINYINT.equals(type)
                    || SMALLINT.equals(type)
                    || INTEGER.equals(type)
                    || BIGINT.equals(type)
                    || DOUBLE.equals(type)
                    || type instanceof VarcharType) {
                TypeUtils.writeNativeValue(type, builder, obj);
            }
        }
    }

    private static Object readObject(Type type, Block block, int position)
    {
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return getArrayFromBlock(elementType, block.getBlock(position));
        }
        else if (type instanceof MapType) {
            return getMapFromBlock(type, block.getBlock(position));
        }
        else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) requireNonNull(TypeUtils.readNativeValue(type, block, position));
                return (type instanceof VarcharType) ? slice.toStringUtf8() : slice.getBytes();
            }

            return TypeUtils.readNativeValue(type, block, position);
        }
    }

    private static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            arrayBuilder.add(readObject(elementType, block, i));
        }
        return arrayBuilder.build();
    }

    private static Map<String, String> metricHeaderToMap(Map<String, String> mapToConvert)
    {
        return ImmutableMap.<String, String>builder().putAll(mapToConvert).build();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!metricsItr.hasNext()) {
            return false;
        }
        fields = metricsItr.next();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return true;
    }

    @Override
    public long getLong(int field)
    {
        Type type = getType(field);
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            Instant dateTime = (Instant) requireNonNull(getFieldValue(field));
            // render with the fixed offset of the Presto server
            int offsetMinutes = dateTime.atZone(ZoneId.systemDefault()).getOffset().getTotalSeconds() / 60;
            return packDateTimeWithZone(dateTime.toEpochMilli(), offsetMinutes);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return (double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice((String) requireNonNull(getFieldValue(field)));
    }

    @Override
    public Object getObject(int field)
    {
        return getFieldValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        if (getFieldValue(field) == null) {
            return true;
        }
        return false;
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        switch (columnIndex) {
            case 0:
                return fields.getLabels();
            case 1:
                return fields.getTimestamp();
            case 2:
                return fields.getValue();
        }
        return new NullPointerException();
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private List<PrometheusStandardizedRow> prometheusResultsInStandardizedForm(List<PrometheusMetricResult> results)
    {
        return results.stream().map(result ->
                result.getTimeSeriesValues().getValues().stream().map(prometheusTimeSeriesValue ->
                        new PrometheusStandardizedRow(
                                getBlockFromMap(columnHandles.get(0).getColumnType(), metricHeaderToMap(result.getMetricHeader())),
                                prometheusTimeSeriesValue.getTimestamp(),
                                Double.parseDouble(prometheusTimeSeriesValue.getValue())))
                        .collect(Collectors.toList()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void close()
    {}
}
