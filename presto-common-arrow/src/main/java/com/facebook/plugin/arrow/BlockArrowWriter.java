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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_TYPE_ERROR;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class BlockArrowWriter
{
    private BlockArrowWriter()
    {
    }

    public static Field prestoToArrowField(ColumnMetadata column)
    {
        Field field;
        Map<String, String> metadata = column.getProperties().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, Object::toString));
        if (column.getType().getTypeSignature().getBase().equals(ARRAY)) {
            ArrayType arrayType = (ArrayType) column.getType();
            Field childField = prestoToArrowField(ColumnMetadata.builder().setName(BaseRepeatedValueVector.DATA_VECTOR_NAME).setType(arrayType.getElementType()).build());
            field = new Field(column.getName(), new FieldType(column.isNullable(), ArrowType.List.INSTANCE, null, metadata), ImmutableList.of(childField));
        }
        else if (column.getType().getTypeSignature().getBase().equals(MAP)) {
            MapType mapType = (MapType) column.getType();
            // NOTE: Arrow key type must be non-nullable
            Field keyField = prestoToArrowField(ColumnMetadata.builder().setName(MapVector.KEY_NAME).setType(mapType.getKeyType()).setNullable(false).build());
            Field valueField = prestoToArrowField(ColumnMetadata.builder().setName(MapVector.VALUE_NAME).setType(mapType.getValueType()).build());
            Field entriesField = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), ImmutableList.of(keyField, valueField));
            field = new Field(column.getName(), new FieldType(column.isNullable(), new ArrowType.Map(false), null, metadata), ImmutableList.of(entriesField));
        }
        else if (column.getType().getTypeSignature().getBase().equals(ROW)) {
            RowType rowType = (RowType) column.getType();
            List<RowType.Field> rowFields = rowType.getFields();

            AtomicInteger childCount = new AtomicInteger();
            List<Field> childFields = rowFields.stream().map(f -> prestoToArrowField(
                            ColumnMetadata.builder().setName(f.getName().orElse(format("$child%s$", childCount.incrementAndGet()))).setType(f.getType()).build()))
                    .collect(toImmutableList());

            field = new Field(column.getName(), new FieldType(column.isNullable(), ArrowType.Struct.INSTANCE, null, metadata), childFields);
        }
        else {
            ArrowType arrowType = prestoToArrowType(column.getType());
            field = new Field(column.getName(), new FieldType(column.isNullable(), arrowType, null, metadata), ImmutableList.of());
        }

        return field;
    }

    public static ArrowType prestoToArrowType(Type type)
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
        throw new ArrowException(ARROW_FLIGHT_TYPE_ERROR, "Unsupported type: " + type);
    }

    public static VectorSchemaRoot createArrowWriters(BufferAllocator allocator, List<ColumnMetadata> columns, ImmutableList.Builder<ArrowVectorWriter> writerBuilder)
    {
        List<Field> fields = columns.stream().map(BlockArrowWriter::prestoToArrowField).collect(Collectors.toList());
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        final List<FieldVector> vectors = root.getFieldVectors();
        checkArgument(vectors.size() == columns.size(), "Unexpected list of vectors: %s", schema);
        for (int i = 0; i < vectors.size(); i++) {
            ColumnMetadata columnMetadata = columns.get(i);
            writerBuilder.add(createArrowWriter(vectors.get(i), columnMetadata.getType()));
        }

        return root;
    }

    private static ArrowVectorWriter createArrowWriter(FieldVector vector, Type type)
    {
        Class<?> javaType = type.getJavaType();

        switch (vector.getMinorType()) {
            case BIT:
                checkArgument(javaType == boolean.class, "Unexpected type for BitVector: %s", type);
                return new ArrowBitWriter((BitVector) vector, new BlockPrimitiveGetter(type));
            case TINYINT:
                checkArgument(javaType == long.class, "Unexpected type for TinyIntVector: %s", type);
                return new ArrowTinyIntWriter((TinyIntVector) vector, new BlockPrimitiveGetter(type));
            case SMALLINT:
                checkArgument(javaType == long.class, "Unexpected type for SmallIntVector: %s", type);
                return new ArrowSmallIntWriter((SmallIntVector) vector, new BlockPrimitiveGetter(type));
            case INT:
                checkArgument(javaType == long.class, "Unexpected type for IntVector: %s", type);
                return new ArrowIntWriter((IntVector) vector, new BlockPrimitiveGetter(type));
            case BIGINT:
                checkArgument(javaType == long.class, "Unexpected type for BigIntVector: %s", type);
                return new ArrowLongWriter((BigIntVector) vector, new BlockPrimitiveGetter(type));
            case FLOAT4:
                checkArgument(javaType == long.class, "Unexpected type for Float4Vector: %s", type);
                return new ArrowRealWriter((Float4Vector) vector, new BlockPrimitiveGetter(type));
            case FLOAT8:
                checkArgument(javaType == double.class, "Unexpected type for Float8Vector: %s", type);
                return new ArrowDoubleWriter((Float8Vector) vector, new BlockPrimitiveGetter(type));
            case DECIMAL:
                checkArgument(type instanceof DecimalType, "Expected DecimalType but got %s", type);
                return new ArrowDecimalWriter((DecimalVector) vector, createDecimalBlockGetter((DecimalType) type, javaType));
            case VARBINARY:
            case VARCHAR:
                checkArgument(javaType == Slice.class, "Unexpected type for BaseVariableWidthVector: %s", type);
                return new ArrowVariableWidthWriter((BaseVariableWidthVector) vector, new BlockSliceGetter(type));
            case DATEDAY:
                checkArgument(javaType == long.class, "Unexpected type for DateDayVector: %s", type);
                return new ArrowDateWriter((DateDayVector) vector, new BlockPrimitiveGetter(type));
            case TIMEMILLI:
                checkArgument(javaType == long.class, "Unexpected type for TimeMilliVector: %s", type);
                return new ArrowTimeWriter((TimeMilliVector) vector, new BlockPrimitiveGetter(type));
            case TIMESTAMPMILLI:
                checkArgument(javaType == long.class, "Unexpected type for TimeStampVector: %s", type);
                return new ArrowTimeStampWriter((TimeStampVector) vector, new BlockPrimitiveGetter(type));
            case LIST:
                checkArgument(type instanceof ArrayType, "Unexpected type for ListVector: %s", type);
                return new ArrowListWriter((ListVector) vector, new BlockArrayGetter((ArrayType) type));
            case MAP:
                checkArgument(type instanceof MapType, "Unexpected type for MapVector: %s", type);
                return new ArrowMapWriter((MapVector) vector, new BlockMapGetter((MapType) type));
            case STRUCT:
                checkArgument(type instanceof RowType, "Unexpected type for StructVector: %s", type);
                return new ArrowStructWriter((StructVector) vector, new BlockRowGetter((RowType) type));
            default:
                throw new ArrowException(ARROW_FLIGHT_TYPE_ERROR, "Unsupported Arrow type: " + vector.getMinorType().name());
        }
    }

    private static BlockValueGetter createDecimalBlockGetter(DecimalType decimalType, Class<?> javaType)
    {
        if (javaType == long.class) {
            return new BlockShortDecimalGetter(decimalType);
        }
        else if (javaType == Slice.class) {
            return new BlockLongDecimalGetter(decimalType);
        }
        throw new ArrowException(ARROW_FLIGHT_TYPE_ERROR, "Unexpected type for DecimalVector: " + decimalType);
    }

    private abstract static class BlockValueGetter
    {
        public boolean getBoolean(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public int getInt(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public long getLong(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public float getFloat(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public double getDouble(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public BigDecimal getDecimal(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public byte[] getBytes(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }

        public Block getChildBlock(Block block, int position)
        {
            throw new UnsupportedOperationException(getClass().getName());
        }
    }

    private static class BlockPrimitiveGetter
            extends BlockValueGetter
    {
        protected Type type;

        public BlockPrimitiveGetter(Type type)
        {
            this.type = type;
        }

        @Override
        public boolean getBoolean(Block block, int position)
        {
            return type.getBoolean(block, position);
        }

        @Override
        public int getInt(Block block, int position)
        {
            return (int) getLong(block, position);
        }

        @Override
        public long getLong(Block block, int position)
        {
            if (block instanceof IntArrayBlock) {
                return block.toLong(position);
            }

            return type.getLong(block, position);
        }

        @Override
        public float getFloat(Block block, int position)
        {
            long value = getLong(block, position);
            return intBitsToFloat(toIntExact(value));
        }

        @Override
        public double getDouble(Block block, int position)
        {
            return type.getDouble(block, position);
        }
    }

    private static class BlockShortDecimalGetter
            extends BlockValueGetter
    {
        protected DecimalType type;

        public BlockShortDecimalGetter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public BigDecimal getDecimal(Block block, int position)
        {
            long value = type.getLong(block, position);
            BigInteger unscaledValue = BigInteger.valueOf(value);
            return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
        }
    }

    private static class BlockLongDecimalGetter
            extends BlockValueGetter
    {
        protected DecimalType type;

        public BlockLongDecimalGetter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public BigDecimal getDecimal(Block block, int position)
        {
            Slice value = type.getSlice(block, position);
            BigInteger unscaledValue = decodeUnscaledValue(value);
            return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
        }
    }

    private static class BlockSliceGetter
            extends BlockValueGetter
    {
        protected Type type;

        public BlockSliceGetter(Type type)
        {
            this.type = type;
        }

        @Override
        public byte[] getBytes(Block block, int position)
        {
            Slice value = type.getSlice(block, position);
            return value.getBytes(0, value.length());
        }
    }

    private static class BlockArrayGetter
            extends BlockValueGetter
    {
        protected ArrayType type;

        public BlockArrayGetter(ArrayType type)
        {
            this.type = type;
        }

        public ArrayType getType()
        {
            return type;
        }

        @Override
        public Block getChildBlock(Block block, int position)
        {
            return type.getObject(block, position);
        }
    }

    private static class BlockMapGetter
            extends BlockValueGetter
    {
        protected MapType type;

        public BlockMapGetter(MapType type)
        {
            this.type = type;
        }

        public MapType getType()
        {
            return type;
        }

        @Override
        public Block getChildBlock(Block block, int position)
        {
            return type.getObject(block, position);
        }
    }

    private static class BlockRowGetter
            extends BlockValueGetter
    {
        protected RowType type;

        public BlockRowGetter(RowType type)
        {
            this.type = type;
        }

        public RowType getType()
        {
            return type;
        }

        @Override
        public Block getChildBlock(Block block, int position)
        {
            return type.getObject(block, position);
        }
    }

    /**
     * Writers for specific Arrow vector types to set values from a Block.
     * Fixed-width vectors are pre-allocated, but setSafe should be used
     * for all other vectors.
     */
    public abstract static class ArrowVectorWriter
    {
        public abstract void writeNull(int index);

        public abstract void writeBlock(int index, Block block, int position);
    }

    private static class ArrowBitWriter
            extends ArrowVectorWriter
    {
        private final BitVector vector;
        protected final BlockValueGetter blockGetter;

        public ArrowBitWriter(BitVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getBoolean(block, position) ? 1 : 0);
        }
    }

    private static class ArrowTinyIntWriter
            extends ArrowVectorWriter
    {
        private final TinyIntVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowTinyIntWriter(TinyIntVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getInt(block, position));
        }
    }

    private static class ArrowSmallIntWriter
            extends ArrowVectorWriter
    {
        private final SmallIntVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowSmallIntWriter(SmallIntVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getInt(block, position));
        }
    }

    private static class ArrowIntWriter
            extends ArrowVectorWriter
    {
        private final IntVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowIntWriter(IntVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getInt(block, position));
        }
    }

    private static class ArrowLongWriter
            extends ArrowVectorWriter
    {
        private final BigIntVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowLongWriter(BigIntVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getLong(block, position));
        }
    }

    private static class ArrowRealWriter
            extends ArrowVectorWriter
    {
        private final Float4Vector vector;
        private final BlockValueGetter blockGetter;

        public ArrowRealWriter(Float4Vector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getFloat(block, position));
        }
    }

    private static class ArrowDoubleWriter
            extends ArrowVectorWriter
    {
        private final Float8Vector vector;
        private final BlockValueGetter blockGetter;

        public ArrowDoubleWriter(Float8Vector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getDouble(block, position));
        }
    }

    private static class ArrowDecimalWriter
            extends ArrowVectorWriter
    {
        private final DecimalVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowDecimalWriter(DecimalVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getDecimal(block, position));
        }
    }

    private static class ArrowVariableWidthWriter
            extends ArrowVectorWriter
    {
        private final BaseVariableWidthVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowVariableWidthWriter(BaseVariableWidthVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.setSafe(index, blockGetter.getBytes(block, position));
        }
    }

    private static class ArrowDateWriter
            extends ArrowVectorWriter
    {
        private final DateDayVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowDateWriter(DateDayVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            // Value is supplied as days since epoch UTC
            vector.set(index, blockGetter.getInt(block, position));
        }
    }

    private static class ArrowTimeWriter
            extends ArrowVectorWriter
    {
        private final TimeMilliVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowTimeWriter(TimeMilliVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getInt(block, position));
        }
    }

    private static class ArrowTimeStampWriter
            extends ArrowVectorWriter
    {
        private final TimeStampVector vector;
        private final BlockValueGetter blockGetter;

        public ArrowTimeStampWriter(TimeStampVector vector, BlockValueGetter blockGetter)
        {
            this.vector = vector;
            this.blockGetter = blockGetter;
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            vector.set(index, blockGetter.getLong(block, position));
        }
    }

    private static class ArrowListWriter
            extends ArrowVectorWriter
    {
        private final ListVector vector;
        private final BlockArrayGetter blockArrayGetter;
        private final ArrowVectorWriter childWriter;

        public ArrowListWriter(ListVector vector, BlockArrayGetter blockArrayGetter)
        {
            this.vector = vector;
            this.blockArrayGetter = blockArrayGetter;
            this.childWriter = createArrowWriter(vector.getDataVector(), blockArrayGetter.getType().getElementType());
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            int dataIndex = vector.startNewValue(index);
            Block elementBlock = blockArrayGetter.getChildBlock(block, position);
            for (int i = 0; i < elementBlock.getPositionCount(); ++i) {
                childWriter.writeBlock(dataIndex + i, elementBlock, i);
            }
            vector.endValue(index, elementBlock.getPositionCount());
        }
    }

    private static class ArrowMapWriter
            extends ArrowVectorWriter
    {
        private final MapVector vector;
        private final StructVector structVector;
        private final BlockMapGetter blockMapGetter;
        private final ArrowVectorWriter keyWriter;
        private final ArrowVectorWriter valueWriter;

        public ArrowMapWriter(MapVector vector, BlockMapGetter blockMapGetter)
        {
            this.vector = vector;
            this.structVector = (StructVector) vector.getDataVector();
            this.blockMapGetter = blockMapGetter;
            this.keyWriter = createArrowWriter((FieldVector) structVector.getChildByOrdinal(0), blockMapGetter.getType().getKeyType());
            this.valueWriter = createArrowWriter((FieldVector) structVector.getChildByOrdinal(1), blockMapGetter.getType().getValueType());
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            Block singleMapBlock = blockMapGetter.getChildBlock(block, position);
            int dataIndex = vector.startNewValue(index);
            int numPairs = singleMapBlock.getPositionCount() / 2;
            for (int i = 0; i < numPairs; ++i) {
                keyWriter.writeBlock(dataIndex + i, singleMapBlock, i * 2);
                valueWriter.writeBlock(dataIndex + i, singleMapBlock, (i * 2) + 1);
                structVector.setIndexDefined(dataIndex + i);
            }
            vector.endValue(index, numPairs);
        }
    }

    private static class ArrowStructWriter
            extends ArrowVectorWriter
    {
        private final StructVector vector;
        private final BlockRowGetter blockRowGetter;
        private final List<ArrowVectorWriter> childWriters;

        public ArrowStructWriter(StructVector vector, BlockRowGetter blockRowGetter)
        {
            this.vector = vector;
            this.blockRowGetter = blockRowGetter;
            ImmutableList.Builder<ArrowVectorWriter> writerBuilder = ImmutableList.builder();
            List<FieldVector> fieldVectors = vector.getChildrenFromFields();
            for (int i = 0; i < fieldVectors.size(); i++) {
                Type childType = blockRowGetter.getType().getTypeParameters().get(i);
                writerBuilder.add(createArrowWriter(fieldVectors.get(i), childType));
            }
            this.childWriters = writerBuilder.build();
        }

        @Override
        public void writeNull(int index)
        {
            vector.setNull(index);
        }

        @Override
        public void writeBlock(int index, Block block, int position)
        {
            Block singleRowBlock = blockRowGetter.getChildBlock(block, position);
            for (int i = 0; i < childWriters.size(); ++i) {
                childWriters.get(i).writeBlock(index, singleRowBlock, i);
            }
            vector.setIndexDefined(index);
        }
    }
}
