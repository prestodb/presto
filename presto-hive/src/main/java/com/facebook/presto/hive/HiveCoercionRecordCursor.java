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

import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;

public class HiveCoercionRecordCursor
        implements RecordCursor
{
    private final RecordCursor delegate;
    private final List<ColumnMapping> columnMappings;
    private final Coercer[] coercers;

    public HiveCoercionRecordCursor(
            List<ColumnMapping> columnMappings,
            TypeManager typeManager,
            RecordCursor delegate)
    {
        requireNonNull(columnMappings, "columns is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = ImmutableList.copyOf(columnMappings);

        int size = columnMappings.size();

        this.coercers = new Coercer[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);

            if (columnMapping.getCoercionFrom().isPresent()) {
                coercers[columnIndex] = createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType());
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return delegate.getTotalBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public Type getType(int field)
    {
        return delegate.getType(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        for (int i = 0; i < columnMappings.size(); i++) {
            if (coercers[i] != null) {
                coercers[i].reset();
            }
        }
        return delegate.advanceNextPosition();
    }

    @Override
    public boolean getBoolean(int field)
    {
        if (coercers[field] == null) {
            return delegate.getBoolean(field);
        }
        return coercers[field].getBoolean(delegate, field);
    }

    @Override
    public long getLong(int field)
    {
        if (coercers[field] == null) {
            return delegate.getLong(field);
        }
        return coercers[field].getLong(delegate, field);
    }

    @Override
    public double getDouble(int field)
    {
        if (coercers[field] == null) {
            return delegate.getDouble(field);
        }
        return coercers[field].getDouble(delegate, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        if (coercers[field] == null) {
            return delegate.getSlice(field);
        }
        return coercers[field].getSlice(delegate, field);
    }

    @Override
    public Object getObject(int field)
    {
        if (coercers[field] == null) {
            return delegate.getObject(field);
        }
        return coercers[field].getObject(delegate, field);
    }

    @Override
    public boolean isNull(int field)
    {
        if (coercers[field] == null) {
            return delegate.isNull(field);
        }
        return coercers[field].isNull(delegate, field);
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @VisibleForTesting
    RecordCursor getRegularColumnRecordCursor()
    {
        return delegate;
    }

    private abstract static class Coercer
    {
        private boolean isNull;
        private boolean loaded;

        private boolean booleanValue;
        private long longValue;
        private double doubleValue;
        private Slice sliceValue;
        private Object objectValue;

        public void reset()
        {
            isNull = false;
            loaded = false;
        }

        public boolean isNull(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return isNull;
        }

        public boolean getBoolean(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return booleanValue;
        }

        public long getLong(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return longValue;
        }

        public double getDouble(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return doubleValue;
        }

        public Slice getSlice(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return sliceValue;
        }

        public Object getObject(RecordCursor delegate, int field)
        {
            assureLoaded(delegate, field);
            return objectValue;
        }

        private void assureLoaded(RecordCursor delegate, int field)
        {
            if (!loaded) {
                isNull = delegate.isNull(field);
                if (!isNull) {
                    coerce(delegate, field);
                }
                loaded = true;
            }
        }

        protected abstract void coerce(RecordCursor delegate, int field);

        protected void setBoolean(boolean value)
        {
            booleanValue = value;
        }

        protected void setLong(long value)
        {
            longValue = value;
        }

        protected void setDouble(double value)
        {
            doubleValue = value;
        }

        protected void setSlice(Slice value)
        {
            sliceValue = value;
        }

        protected void setObject(Object value)
        {
            objectValue = value;
        }

        protected void setIsNull(boolean isNull)
        {
            this.isNull = isNull;
        }
    }

    private static Coercer createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer();
        }
        else if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer(toHiveType);
        }
        else if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer();
        }
        else if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    private static class IntegerNumberUpscaleCoercer
            extends Coercer
    {
        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            setLong(delegate.getLong(field));
        }
    }

    private static class IntegerNumberToVarcharCoercer
            extends Coercer
    {
        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            setSlice(utf8Slice(valueOf(delegate.getLong(field))));
        }
    }

    private static class FloatToDoubleCoercer
            extends Coercer
    {
        @Override
        protected void coerce(RecordCursor delegate, int field)
        {
            setDouble(intBitsToFloat((int) delegate.getLong(field)));
        }
    }

    private static class VarcharToIntegerNumberCoercer
            extends Coercer
    {
        private final long maxValue;
        private final long minValue;

        public VarcharToIntegerNumberCoercer(HiveType type)
        {
            if (type.equals(HIVE_BYTE)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (type.equals(HIVE_SHORT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (type.equals(HIVE_INT)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (type.equals(HIVE_LONG)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from varchar to %s", type));
            }
        }

        @Override
        public void coerce(RecordCursor delegate, int field)
        {
            try {
                long value = Long.parseLong(delegate.getSlice(field).toStringUtf8());
                if (minValue <= value && value <= maxValue) {
                    setLong(value);
                }
                else {
                    setIsNull(true);
                }
            }
            catch (NumberFormatException e) {
                setIsNull(true);
            }
        }
    }
}
