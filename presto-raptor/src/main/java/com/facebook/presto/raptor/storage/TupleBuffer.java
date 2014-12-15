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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TupleBuffer
{
    private final List<Type> types;
    private final int sampleWeightField;
    private final int[] offsets;
    private final boolean[] valueIsNull;
    private final SliceOutput sliceOutput;

    private int outputField;
    private long sampleWeight;

    public TupleBuffer(List<Type> types, int sampleWeightField)
    {
        this(types, sampleWeightField, 256);
    }

    public TupleBuffer(List<Type> types, int sampleWeightField, int estimatedSize)
    {
        checkNotNull(types, "types is null");
        checkArgument(estimatedSize >= 0, "Estimated size must be greater than or equal to zero: %s", estimatedSize);

        this.types = ImmutableList.copyOf(types);
        this.sampleWeightField = sampleWeightField;
        this.sliceOutput = new DynamicSliceOutput(estimatedSize);
        this.offsets = new int[types.size()];
        this.valueIsNull = new boolean[types.size()];
        this.outputField = 0;
    }

    public void reset()
    {
        outputField = 0;
        sliceOutput.reset();
        Arrays.fill(valueIsNull, false);
        Arrays.fill(offsets, -1);
    }

    public TupleBuffer appendNull()
    {
        checkElementIndex(outputField, types.size());
        offsets[outputField] = sliceOutput.size();
        valueIsNull[outputField] = true;
        outputField++;
        return this;
    }

    public TupleBuffer appendLong(long value)
    {
        checkElementIndex(outputField, types.size());
        Type type = types.get(outputField);
        checkState(type.getJavaType() == long.class, "Cannot append LONG for type %s", type);
        offsets[outputField] = sliceOutput.size();
        sliceOutput.appendLong(value);
        outputField++;
        return this;
    }

    public TupleBuffer appendDouble(double value)
    {
        checkElementIndex(outputField, types.size());
        Type type = types.get(outputField);
        checkState(type.getJavaType() == double.class, "Cannot append DOUBLE for type %s", type);
        offsets[outputField] = sliceOutput.size();
        sliceOutput.appendDouble(value);
        outputField++;
        return this;
    }

    public TupleBuffer appendBoolean(boolean value)
    {
        checkElementIndex(outputField, types.size());
        Type type = types.get(outputField);
        checkState(type.getJavaType() == boolean.class, "Cannot append BOOLEAN for type %s", type);
        offsets[outputField] = sliceOutput.size();
        sliceOutput.appendByte(value ? 1 : 0);
        outputField++;
        return this;
    }

    public TupleBuffer appendSlice(Slice value)
    {
        checkElementIndex(outputField, types.size());
        Type type = types.get(outputField);
        checkState(type.getJavaType() == Slice.class, "Cannot append SLICE for type %s", type);
        offsets[outputField] = sliceOutput.size();
        sliceOutput.appendInt(value.length());
        sliceOutput.appendBytes(value);
        outputField++;
        return this;
    }

    public int getFieldCount()
    {
        return types.size();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Type getType(int field)
    {
        checkElementIndex(field, types.size());
        return types.get(field);
    }

    public boolean isNull(int field)
    {
        checkElementIndex(field, types.size());
        checkArgument(isFinalized());
        return valueIsNull[field];
    }

    public long getLong(int field)
    {
        checkElementIndex(field, types.size());
        checkArgument(isFinalized());
        Type type = types.get(field);
        checkState(type.getJavaType() == long.class, "Cannot get LONG for type %s", type);
        return sliceOutput.slice().getLong(offsets[field]);
    }

    public double getDouble(int field)
    {
        checkElementIndex(field, types.size());
        checkArgument(isFinalized());
        Type type = types.get(field);
        checkState(type.getJavaType() == double.class, "Cannot get DOUBLE for type %s", type);
        return sliceOutput.slice().getDouble(offsets[field]);
    }

    public boolean getBoolean(int field)
    {
        checkElementIndex(field, types.size());
        checkArgument(isFinalized());
        Type type = types.get(field);
        checkState(type.getJavaType() == boolean.class, "Cannot get BOOLEAN for type %s", type);
        return sliceOutput.slice().getByte(offsets[field]) != 0;
    }

    public Slice getSlice(int field)
    {
        checkElementIndex(field, types.size());
        checkArgument(isFinalized());
        Type type = types.get(field);
        checkState(type.getJavaType() == Slice.class, "Cannot get SLICE for type %s", type);
        int length = sliceOutput.slice().getInt(offsets[field]);
        return sliceOutput.slice().slice(offsets[field] + SizeOf.SIZE_OF_INT, length);
    }

    public int currentField()
    {
        return outputField;
    }

    public boolean isFinalized()
    {
        if (sampleWeightField >= 0) {
            return outputField + 1 == getFieldCount();
        }
        return outputField == getFieldCount();
    }

    public Object getField(int field)
    {
        checkElementIndex(field, types.size());
        if (sampleWeightField >= 0 && sampleWeightField == field) {
            return sampleWeight;
        }

        if (isNull(field)) {
            return null;
        }
        Type type = types.get(field);
        if (type.equals(BIGINT) ||
                type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP) ||
                type.equals(INTERVAL_YEAR_MONTH) ||
                type.equals(INTERVAL_DAY_TIME)) {
            return getLong(field);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return getDouble(field);
        }
        if (type.equals(BooleanType.BOOLEAN)) {
            return getBoolean(field);
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return new String(getSlice(field).getBytes());
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return getSlice(field).getBytes();
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public void setSampleWeight(long sampleWeight)
    {
        this.sampleWeight = sampleWeight;
    }

    public long getSampleWeight()
    {
        return sampleWeight;
    }
}
