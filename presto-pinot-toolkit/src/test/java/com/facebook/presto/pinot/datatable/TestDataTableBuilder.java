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
package com.facebook.presto.pinot.datatable;

import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.common.datatable.DataTableUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestDataTableBuilder
{
    private final Object2IntOpenHashMap<String> dictionary = new Object2IntOpenHashMap<>();
    protected final DataSchema dataSchema;
    protected final int[] columnOffsets;
    protected final int rowSizeInBytes;
    protected final ByteArrayOutputStream fixedSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
    protected final ByteArrayOutputStream variableSizeDataByteArrayOutputStream = new ByteArrayOutputStream();
    protected final DataOutputStream variableSizeDataOutputStream =
            new DataOutputStream(variableSizeDataByteArrayOutputStream);

    protected int numRows;
    protected ByteBuffer currentRowDataByteBuffer;

    public TestDataTableBuilder(DataSchema dataSchema)
    {
        this.dataSchema = dataSchema;
        this.columnOffsets = new int[dataSchema.size()];
        this.rowSizeInBytes =
                DataTableUtils.computeColumnOffsets(dataSchema, columnOffsets, DataTableFactory.VERSION_4);
    }

    public void setColumn(int colId, String value)
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        int dictId = dictionary.computeIfAbsent(value, k -> dictionary.size());
        currentRowDataByteBuffer.putInt(dictId);
    }

    public void setColumn(int colId, ByteArray value)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        byte[] bytes = value.getBytes();
        currentRowDataByteBuffer.putInt(bytes.length);
        variableSizeDataByteArrayOutputStream.write(bytes);
    }

    public void setColumn(int colId, String[] values)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        currentRowDataByteBuffer.putInt(values.length);
        for (String value : values) {
            int dictId = dictionary.computeIfAbsent(value, k -> dictionary.size());
            variableSizeDataOutputStream.writeInt(dictId);
        }
    }

    public DataTable build()
    {
        String[] reverseDictionary = new String[dictionary.size()];
        for (Object2IntMap.Entry<String> entry : dictionary.object2IntEntrySet()) {
            reverseDictionary[entry.getIntValue()] = entry.getKey();
        }
        return new DataTableImplV4(numRows, dataSchema, reverseDictionary,
                fixedSizeDataByteArrayOutputStream.toByteArray(),
                variableSizeDataByteArrayOutputStream.toByteArray());
    }

    public void startRow()
    {
        numRows++;
        currentRowDataByteBuffer = ByteBuffer.allocate(rowSizeInBytes);
    }

    public void setColumn(int colId, int value)
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(value);
    }

    public void setColumn(int colId, long value)
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putLong(value);
    }

    public void setColumn(int colId, float value)
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putFloat(value);
    }

    public void setColumn(int colId, double value)
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putDouble(value);
    }

    public void setColumn(int colId, BigDecimal value)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        byte[] bytes = BigDecimalUtils.serialize(value);
        currentRowDataByteBuffer.putInt(bytes.length);
        variableSizeDataByteArrayOutputStream.write(bytes);
    }

    public void setColumn(int colId, @Nullable Object value)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        if (value == null) {
            currentRowDataByteBuffer.putInt(0);
            variableSizeDataOutputStream.writeInt(DataTable.CustomObject.NULL_TYPE_VALUE);
        }
        else {
            int objectTypeValue = ObjectType.getObjectType(value).getValue();
            byte[] bytes = serializeObject(value);
            currentRowDataByteBuffer.putInt(bytes.length);
            variableSizeDataOutputStream.writeInt(objectTypeValue);
            variableSizeDataByteArrayOutputStream.write(bytes);
        }
    }

    private byte[] serializeObject(Object value)
    {
        if (value instanceof String) {
            return ((String) value).getBytes(UTF_8);
        }
        else if (value instanceof Long) {
            return Longs.toByteArray((Long) value);
        }
        else if (value instanceof Double) {
            return Longs.toByteArray(Double.doubleToRawLongBits((Double) value));
        }
        else if (value instanceof BigDecimal) {
            return BigDecimalUtils.serialize((BigDecimal) value);
        }
        throw new UnsupportedOperationException("Unsupported object type: " + value.getClass().getName());
    }

    public void setColumn(int colId, int[] values)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        currentRowDataByteBuffer.putInt(values.length);
        for (int value : values) {
            variableSizeDataOutputStream.writeInt(value);
        }
    }

    public void setColumn(int colId, long[] values)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        currentRowDataByteBuffer.putInt(values.length);
        for (long value : values) {
            variableSizeDataOutputStream.writeLong(value);
        }
    }

    public void setColumn(int colId, float[] values)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        currentRowDataByteBuffer.putInt(values.length);
        for (float value : values) {
            variableSizeDataOutputStream.writeFloat(value);
        }
    }

    public void setColumn(int colId, double[] values)
            throws IOException
    {
        currentRowDataByteBuffer.position(columnOffsets[colId]);
        currentRowDataByteBuffer.putInt(variableSizeDataByteArrayOutputStream.size());
        currentRowDataByteBuffer.putInt(values.length);
        for (double value : values) {
            variableSizeDataOutputStream.writeDouble(value);
        }
    }

    public void finishRow()
            throws IOException
    {
        fixedSizeDataByteArrayOutputStream.write(currentRowDataByteBuffer.array());
    }

    public enum ObjectType
    {
        // NOTE: DO NOT change the value, we rely on the value to indicate the object type
        String(0),
        Long(1),
        Double(2);

        private final int value;

        ObjectType(int value)
        {
            this.value = value;
        }

        public static ObjectType getObjectType(Object value)
        {
            if (value instanceof String) {
                return ObjectType.String;
            }
            else if (value instanceof Long) {
                return ObjectType.Long;
            }
            else if (value instanceof Double) {
                return ObjectType.Double;
            }
            else {
                throw new IllegalArgumentException("Unsupported type of value: " + value.getClass().getSimpleName());
            }
        }

        public int getValue()
        {
            return value;
        }
    }
}
