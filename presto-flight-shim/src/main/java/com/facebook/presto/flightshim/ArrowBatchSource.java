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
package com.facebook.presto.flightshim;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Closeable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ArrowBatchSource
        implements Closeable
{
    private final List<ColumnMetadata> columns;
    private final RecordCursor cursor;
    private final VectorSchemaRoot root;
    private final List<ArrowShimWriter> writers;
    private final int maxRowsPerBatch;
    private boolean closed;

    public ArrowBatchSource(BufferAllocator allocator, List<ColumnMetadata> columns, RecordCursor cursor, int maxRowsPerBatch)
    {
        this.columns = unmodifiableList(new ArrayList<>(requireNonNull(columns, "columns is null")));
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.maxRowsPerBatch = maxRowsPerBatch;
        this.root = createVectorSchemaRoot(allocator, columns);
        this.writers = createArrowWriters(root);
    }

    public VectorSchemaRoot getVectorSchemaRoot()
    {
        return root;
    }

    /**
     * Loads the next record batch from the source.
     * Returns false if there are no more batches from the source.
     */
    public boolean nextBatch()
    {
        // Release previous buffers
        root.clear();

        if (closed) {
            return false;
        }

        // Reserve capacity for next batch
        allocateVectorCapacity(root, maxRowsPerBatch);

        int i;
        for (i = 0; i < maxRowsPerBatch; ++i) {
            if (!cursor.advanceNextPosition()) {
                closed = true;
                break;
            }

            for (int column = 0; column < writers.size(); column++) {
                ArrowShimWriter writer = writers.get(column);
                if (cursor.isNull(column)) {
                    writer.writeNull(i);
                }
                else {
                    ColumnMetadata columnMetadata = columns.get(column);
                    Type type = columnMetadata.getType();
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        writer.writeBoolean(i, cursor.getBoolean(column));
                    }
                    else if (javaType == long.class) {
                        writer.writeLong(i, cursor.getLong(column));
                    }
                    else if (javaType == double.class) {
                        writer.writeDouble(i, cursor.getDouble(column));
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = cursor.getSlice(column);
                        writer.writeSlice(i, slice, 0, slice.length());
                    }
                    else {
                        // TODO handle Object cursor.getObject(column)
                        throw new UnsupportedOperationException();
                    }
                }
            }
        }

        root.setRowCount(i);
        return i > 0;
    }

    @Override
    public void close()
    {
        root.close();
        cursor.close();
    }

    private static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator, List<ColumnMetadata> columns)
    {
        List<Field> fields = columns.stream().map(column -> {
            Map<String, String> metadata = column.getProperties().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Object::toString));
            ArrowType arrowType = prestoToArrowType(column.getType());
            return new Field(column.getName(), new FieldType(column.isNullable(), arrowType, null, metadata), ImmutableList.of());
        }).collect(Collectors.toList());
        Schema schema = new Schema(fields);

        return VectorSchemaRoot.create(schema, allocator);
    }

    private static ArrowType prestoToArrowType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        else if (type.equals(TinyintType.TINYINT)) {
            return Types.MinorType.TINYINT.getType();
        }
        else if (type.equals(SmallintType.SMALLINT)) {
            return Types.MinorType.SMALLINT.getType();
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return Types.MinorType.INT.getType();
        }
        else if (type.equals(BigintType.BIGINT)) {
            return Types.MinorType.BIGINT.getType();
        }
        else if (type.equals(RealType.REAL)) {
            return Types.MinorType.FLOAT4.getType();
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return Types.MinorType.FLOAT8.getType();
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return ArrowType.Binary.INSTANCE;
        }
        else if (type instanceof CharType) {
            return ArrowType.Utf8.INSTANCE;
        }
        else if (isVarcharType(type)) {
            return ArrowType.Utf8.INSTANCE;
        }
        else if (type instanceof DateType) {
            return Types.MinorType.DATEDAY.getType();
        }
        else if (type instanceof TimeType) {
            return Types.MinorType.TIMEMILLI.getType();
        }
        else if (type instanceof TimestampType) {
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            // Read as plain timestamp, timezone not supplied with type
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static List<ArrowShimWriter> createArrowWriters(VectorSchemaRoot root)
    {
        final List<FieldVector> vectors = root.getFieldVectors();
        return vectors.stream().map(ArrowBatchSource::createArrowWriter).collect(Collectors.toList());
    }

    private static ArrowShimWriter createArrowWriter(FieldVector vector)
    {
        switch (vector.getMinorType()) {
            case BIT:
                return new ArrowShimBitWriter((BitVector) vector);
            case TINYINT:
                return new ArrowShimTinyIntWriter((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowShimSmallIntWriter((SmallIntVector) vector);
            case INT:
                return new ArrowShimIntWriter((IntVector) vector);
            case BIGINT:
                return new ArrowShimLongWriter((BigIntVector) vector);
            case FLOAT4:
                return new ArrowShimRealWriter((Float4Vector) vector);
            case FLOAT8:
                return new ArrowShimDoubleWriter((Float8Vector) vector);
            case DECIMAL:
                return new ArrowShimDecimalWriter((DecimalVector) vector);
            case VARBINARY:
            case VARCHAR:
                return new ArrowShimVariableWidthWriter((BaseVariableWidthVector) vector);
            case DATEDAY:
                return new ArrowShimDateWriter((DateDayVector) vector);
            case TIMEMILLI:
                return new ArrowShimTimeWriter((TimeMilliVector) vector);
            case TIMESTAMPMILLI:
                return new ArrowShimTimeStampWriter((TimeStampVector) vector);
            default:
                throw new UnsupportedOperationException("Unsupported Arrow type: " + vector.getMinorType().name());
        }
    }

    private static void allocateVectorCapacity(VectorSchemaRoot root, int capacity)
    {
        for (ValueVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(capacity);
            AllocationHelper.allocateNew(vector, capacity);
        }
    }

    private abstract static class ArrowShimWriter
    {
        public abstract void writeNull(int index);

        public void writeBoolean(int index, boolean value)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public void writeLong(int index, long value)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public void writeDouble(int index, double value)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public void writeSlice(int index, Slice value, int offset, int length)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }
    }

    private abstract static class ArrowFixedWidthShimWriter
            extends ArrowShimWriter
    {
        public abstract BaseFixedWidthVector getVector();

        @Override
        public void writeNull(int index)
        {
            getVector().setNull(index);
        }
    }

    private static class ArrowShimBitWriter
            extends ArrowFixedWidthShimWriter
    {
        private final BitVector vector;

        public ArrowShimBitWriter(BitVector vector)
        {
            this.vector = vector;
        }

        @Override
        public BitVector getVector()
        {
            return vector;
        }

        @Override
        public void writeBoolean(int index, boolean value)
        {
            vector.set(index, value ? 1 : 0);
        }
    }

    private static class ArrowShimTinyIntWriter
            extends ArrowFixedWidthShimWriter
    {
        private final TinyIntVector vector;

        public ArrowShimTinyIntWriter(TinyIntVector vector)
        {
            this.vector = vector;
        }

        @Override
        public TinyIntVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, (int) value);
        }
    }

    private static class ArrowShimSmallIntWriter
            extends ArrowFixedWidthShimWriter
    {
        private final SmallIntVector vector;

        public ArrowShimSmallIntWriter(SmallIntVector vector)
        {
            this.vector = vector;
        }

        @Override
        public SmallIntVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, (int) value);
        }
    }

    private static class ArrowShimIntWriter
            extends ArrowFixedWidthShimWriter
    {
        private final IntVector vector;

        public ArrowShimIntWriter(IntVector vector)
        {
            this.vector = vector;
        }

        @Override
        public IntVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, (int) value);
        }
    }

    private static class ArrowShimLongWriter
            extends ArrowFixedWidthShimWriter
    {
        private final BigIntVector vector;

        public ArrowShimLongWriter(BigIntVector vector)
        {
            this.vector = vector;
        }

        @Override
        public BigIntVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, value);
        }
    }

    private static class ArrowShimRealWriter
            extends ArrowFixedWidthShimWriter
    {
        private final Float4Vector vector;

        public ArrowShimRealWriter(Float4Vector vector)
        {
            this.vector = vector;
        }

        @Override
        public Float4Vector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, intBitsToFloat(toIntExact(value)));
        }
    }

    private static class ArrowShimDoubleWriter
            extends ArrowFixedWidthShimWriter
    {
        private final Float8Vector vector;

        public ArrowShimDoubleWriter(Float8Vector vector)
        {
            this.vector = vector;
        }

        @Override
        public Float8Vector getVector()
        {
            return vector;
        }

        @Override
        public void writeDouble(int index, double value)
        {
            vector.set(index, value);
        }
    }

    private static class ArrowShimDecimalWriter
            extends ArrowFixedWidthShimWriter
    {
        private final DecimalVector vector;

        public ArrowShimDecimalWriter(DecimalVector vector)
        {
            this.vector = vector;
        }

        @Override
        public DecimalVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            // ShortDecimalType
            BigInteger unscaledValue = BigInteger.valueOf(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, vector.getScale(), new MathContext(vector.getPrecision()));
            vector.set(index, bigDecimal);
        }

        @Override
        public void writeSlice(int index, Slice value, int offset, int length)
        {
            // LongDecimalType
            BigInteger unscaledValue = decodeUnscaledValue(value);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, vector.getScale(), new MathContext(vector.getPrecision()));
            vector.set(index, bigDecimal);
        }
    }

    private static class ArrowShimVariableWidthWriter
            extends ArrowShimWriter
    {
        private final BaseVariableWidthVector vector;

        public ArrowShimVariableWidthWriter(BaseVariableWidthVector vector)
        {
            this.vector = vector;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeSlice(int index, Slice value, int offset, int length)
        {
            vector.setSafe(index, value.getBytes(offset, length));
        }
    }

    private static class ArrowShimDateWriter
            extends ArrowFixedWidthShimWriter
    {
        private final DateDayVector vector;

        public ArrowShimDateWriter(DateDayVector vector)
        {
            this.vector = vector;
        }

        @Override
        public DateDayVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            // Value is supplied as days since epoch UTC
            vector.set(index, (int) value);
        }
    }

    private static class ArrowShimTimeWriter
            extends ArrowFixedWidthShimWriter
    {
        private final TimeMilliVector vector;

        public ArrowShimTimeWriter(TimeMilliVector vector)
        {
            this.vector = vector;
        }

        @Override
        public TimeMilliVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, (int) value);
        }
    }

    private static class ArrowShimTimeStampWriter
            extends ArrowFixedWidthShimWriter
    {
        private final TimeStampVector vector;

        public ArrowShimTimeStampWriter(TimeStampVector vector)
        {
            this.vector = vector;
        }

        @Override
        public TimeStampVector getVector()
        {
            return vector;
        }

        @Override
        public void writeLong(int index, long value)
        {
            vector.set(index, (int) value);
        }
    }
}
