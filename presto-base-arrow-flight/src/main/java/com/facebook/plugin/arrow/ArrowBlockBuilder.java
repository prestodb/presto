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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_TYPE_ERROR;
import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ArrowBlockBuilder
{
    private final TypeManager typeManager;

    @Inject
    public ArrowBlockBuilder(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }
    public Block buildBlockFromFieldVector(FieldVector vector, Type type, DictionaryProvider dictionaryProvider)
    {
        // Use Arrow dictionary to create a DictionaryBlock
        if (dictionaryProvider != null && vector.getField().getDictionary() != null) {
            Dictionary dictionary = dictionaryProvider.lookup(vector.getField().getDictionary().getId());
            if (dictionary != null) {
                Type prestoType = getPrestoTypeFromArrowField(dictionary.getVector().getField());
                BlockBuilder dictionaryBuilder = prestoType.createBlockBuilder(null, vector.getValueCount());
                assignBlockFromValueVector(dictionary.getVector(), prestoType, dictionaryBuilder, 0, dictionary.getVector().getValueCount());
                return buildDictionaryBlock(vector, dictionaryBuilder.build());
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        assignBlockFromValueVector(vector, type, builder, 0, vector.getValueCount());
        return builder.build();
    }

    protected Type getPrestoTypeFromArrowField(Field field)
    {
        switch (field.getType().getTypeID()) {
            case Int:
                ArrowType.Int intType = (ArrowType.Int) field.getType();
                return getPrestoTypeForArrowIntType(intType);
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return VarbinaryType.VARBINARY;
            case Date:
                return DateType.DATE;
            case Timestamp:
                return TimestampType.TIMESTAMP;
            case Utf8:
            case LargeUtf8:
                return VarcharType.VARCHAR;
            case FloatingPoint:
                ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) field.getType();
                return getPrestoTypeForArrowFloatingPointType(floatingPoint);
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                return DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
            case Bool:
                return BooleanType.BOOLEAN;
            case Time:
                return TimeType.TIME;
            case List: {
                List<Field> children = field.getChildren();
                checkArgument(children.size() == 1, "Arrow List expected to have 1 child Field, got: " + children.size());
                return new ArrayType(getPrestoTypeFromArrowField(field.getChildren().get(0)));
            }
            case Map: {
                List<Field> children = field.getChildren();
                checkArgument(children.size() == 1, "Arrow Map expected to have 1 child Field for entries, got: " + children.size());
                Field entryField = children.get(0);
                checkArgument(entryField.getChildren().size() == 2, "Arrow Map entries expected to have 2 child Fields, got: " + children.size());
                Type keyType = getPrestoTypeFromArrowField(entryField.getChildren().get(0));
                Type valueType = getPrestoTypeFromArrowField(entryField.getChildren().get(1));
                return typeManager.getType(parseTypeSignature(format("map(%s,%s)", keyType.getTypeSignature(), valueType.getTypeSignature())));
            }
            case Struct: {
                List<RowType.Field> children = field.getChildren().stream().map(child -> new RowType.Field(Optional.of(child.getName()), getPrestoTypeFromArrowField(child))).collect(toImmutableList());
                return RowType.from(children);
            }
            default:
                throw new UnsupportedOperationException("The data type " + field.getType().getTypeID() + " is not supported.");
        }
    }

    private Type getPrestoTypeForArrowFloatingPointType(ArrowType.FloatingPoint floatingPoint)
    {
        switch (floatingPoint.getPrecision()) {
            case SINGLE:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            default:
                throw new ArrowException(ARROW_FLIGHT_TYPE_ERROR, "Unexpected floating point precision: " + floatingPoint.getPrecision());
        }
    }

    private Type getPrestoTypeForArrowIntType(ArrowType.Int intType)
    {
        switch (intType.getBitWidth()) {
            case 64:
                return BigintType.BIGINT;
            case 32:
                return IntegerType.INTEGER;
            case 16:
                return SmallintType.SMALLINT;
            case 8:
                return TinyintType.TINYINT;
            default:
                throw new ArrowException(ARROW_FLIGHT_TYPE_ERROR, "Unexpected bit width: " + intType.getBitWidth());
        }
    }

    private DictionaryBlock buildDictionaryBlock(FieldVector fieldVector, Block dictionaryblock)
    {
        if (fieldVector instanceof IntVector) {
            // Get the Arrow indices vector
            IntVector indicesVector = (IntVector) fieldVector;
            int[] ids = new int[indicesVector.getValueCount()];
            for (int i = 0; i < indicesVector.getValueCount(); i++) {
                ids[i] = indicesVector.get(i);
            }
            return new DictionaryBlock(ids.length, dictionaryblock, ids);
        }
        else if (fieldVector instanceof SmallIntVector) {
            // Get the SmallInt indices vector
            SmallIntVector smallIntIndicesVector = (SmallIntVector) fieldVector;
            int[] ids = new int[smallIntIndicesVector.getValueCount()];
            for (int i = 0; i < smallIntIndicesVector.getValueCount(); i++) {
                ids[i] = smallIntIndicesVector.get(i);
            }
            return new DictionaryBlock(ids.length, dictionaryblock, ids);
        }
        else if (fieldVector instanceof TinyIntVector) {
            // Get the TinyInt indices vector
            TinyIntVector tinyIntIndicesVector = (TinyIntVector) fieldVector;
            int[] ids = new int[tinyIntIndicesVector.getValueCount()];
            for (int i = 0; i < tinyIntIndicesVector.getValueCount(); i++) {
                ids[i] = tinyIntIndicesVector.get(i);
            }
            return new DictionaryBlock(ids.length, dictionaryblock, ids);
        }
        else {
            // Handle the case where the FieldVector is of an unsupported type
            throw new IllegalArgumentException("Unsupported FieldVector type: " + fieldVector.getClass());
        }
    }

    private void assignBlockFromValueVector(ValueVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (vector instanceof BitVector) {
            assignBlockFromBitVector((BitVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TinyIntVector) {
            assignBlockFromTinyIntVector((TinyIntVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof IntVector) {
            assignBlockFromIntVector((IntVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof SmallIntVector) {
            assignBlockFromSmallIntVector((SmallIntVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof BigIntVector) {
            assignBlockFromBigIntVector((BigIntVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof DecimalVector) {
            assignBlockFromDecimalVector((DecimalVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof NullVector) {
            assignBlockFromNullVector((NullVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeStampMicroVector) {
            assignBlockFromTimeStampMicroVector((TimeStampMicroVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeStampMilliVector) {
            assignBlockFromTimeStampMilliVector((TimeStampMilliVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof Float4Vector) {
            assignBlockFromFloat4Vector((Float4Vector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof Float8Vector) {
            assignBlockFromFloat8Vector((Float8Vector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof VarCharVector) {
            if (type instanceof CharType) {
                assignCharTypeBlockFromVarcharVector((VarCharVector) vector, type, builder, startIndex, endIndex);
            }
            else if (type instanceof TimeType) {
                assignTimeTypeBlockFromVarcharVector((VarCharVector) vector, type, builder, startIndex, endIndex);
            }
            else {
                assignBlockFromVarCharVector((VarCharVector) vector, type, builder, startIndex, endIndex);
            }
        }
        else if (vector instanceof VarBinaryVector) {
            assignBlockFromVarBinaryVector((VarBinaryVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof DateDayVector) {
            assignBlockFromDateDayVector((DateDayVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof DateMilliVector) {
            assignBlockFromDateMilliVector((DateMilliVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeMilliVector) {
            assignBlockFromTimeMilliVector((TimeMilliVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeSecVector) {
            assignBlockFromTimeSecVector((TimeSecVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeStampSecVector) {
            assignBlockFromTimeStampSecVector((TimeStampSecVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeMicroVector) {
            assignBlockFromTimeMicroVector((TimeMicroVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof TimeStampMilliTZVector) {
            assignBlockFromTimeMilliTZVector((TimeStampMilliTZVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof MapVector) {
            // NOTE: MapVector is also instanceof ListVector, so check for Map first
            assignBlockFromMapVector((MapVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof ListVector) {
            assignBlockFromListVector((ListVector) vector, type, builder, startIndex, endIndex);
        }
        else if (vector instanceof StructVector) {
            assignBlockFromStructVector((StructVector) vector, type, builder, startIndex, endIndex);
        }
        else {
            throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass());
        }
    }

    public void assignBlockFromBitVector(BitVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeBoolean(builder, vector.get(i) == 1);
            }
        }
    }

    public void assignBlockFromIntVector(IntVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromSmallIntVector(SmallIntVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromTinyIntVector(TinyIntVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromBigIntVector(BigIntVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromDecimalVector(DecimalVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof DecimalType)) {
            throw new IllegalArgumentException("Type must be a DecimalType for DecimalVector");
        }

        DecimalType decimalType = (DecimalType) type;

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BigDecimal decimal = vector.getObject(i); // Get the BigDecimal value
                if (decimalType.isShort()) {
                    builder.writeLong(decimal.unscaledValue().longValue());
                }
                else {
                    Slice slice = Decimals.encodeScaledValue(decimal);
                    decimalType.writeSlice(builder, slice, 0, slice.length());
                }
            }
        }
    }

    public void assignBlockFromNullVector(NullVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            builder.appendNull();
        }
    }

    public void assignBlockFromTimeStampMicroVector(TimeStampMicroVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long micros = vector.get(i);
                long millis = TimeUnit.MICROSECONDS.toMillis(micros);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromTimeStampMilliVector(TimeStampMilliVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromFloat8Vector(Float8Vector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeDouble(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromFloat4Vector(Float4Vector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int intBits = Float.floatToIntBits(vector.get(i));
                type.writeLong(builder, intBits);
            }
        }
    }

    public void assignBlockFromVarBinaryVector(VarBinaryVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                byte[] value = vector.get(i);
                type.writeSlice(builder, Slices.wrappedBuffer(value));
            }
        }
    }

    public void assignBlockFromVarCharVector(VarCharVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof VarcharType)) {
            throw new IllegalArgumentException("Expected VarcharType but got " + type.getClass().getName());
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                // Directly create a Slice from the raw byte array
                byte[] rawBytes = vector.get(i);
                Slice slice = Slices.wrappedBuffer(rawBytes);
                // Write the Slice directly to the builder
                type.writeSlice(builder, slice);
            }
        }
    }

    public void assignBlockFromDateDayVector(DateDayVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
    }

    public void assignBlockFromDateMilliVector(DateMilliVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                DateType dateType = (DateType) type;
                long days = TimeUnit.MILLISECONDS.toDays(vector.get(i));
                dateType.writeLong(builder, days);
            }
        }
    }

    public void assignBlockFromTimeSecVector(TimeSecVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromTimeMilliVector(TimeMilliVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromTimeMicroVector(TimeMicroVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimemicroVector");
        }
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long micro = TimeUnit.MICROSECONDS.toMillis(value);
                type.writeLong(builder, micro);
            }
        }
    }

    public void assignBlockFromTimeStampSecVector(TimeStampSecVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampSecVector");
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignCharTypeBlockFromVarcharVector(VarCharVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String value = new String(vector.get(i), StandardCharsets.UTF_8);
                type.writeSlice(builder, Slices.utf8Slice(CharMatcher.is(' ').trimTrailingFrom(value)));
            }
        }
    }

    public void assignTimeTypeBlockFromVarcharVector(VarCharVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String timeString = new String(vector.get(i), StandardCharsets.UTF_8);
                LocalTime time = LocalTime.parse(timeString);
                long millis = Duration.between(LocalTime.MIN, time).toMillis();
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromTimeMilliTZVector(TimeStampMilliTZVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampMilliTZVector");
        }

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
    }

    public void assignBlockFromListVector(ListVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof ArrayType)) {
            throw new IllegalArgumentException("Type must be an ArrayType for ListVector");
        }

        ArrayType arrayType = (ArrayType) type;
        Type elementType = arrayType.getElementType();

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BlockBuilder elementBuilder = builder.beginBlockEntry();
                assignBlockFromValueVector(
                        vector.getDataVector(), elementType, elementBuilder, vector.getElementStartIndex(i), vector.getElementEndIndex(i));
                builder.closeEntry();
            }
        }
    }

    public void assignBlockFromMapVector(MapVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof MapType)) {
            throw new IllegalArgumentException("Type must be a MapType for MapVector");
        }

        MapType mapType = (MapType) type;
        StructVector entryVector = (StructVector) vector.getDataVector();
        ValueVector keyVector = entryVector.getChildByOrdinal(0);
        ValueVector valueVector = entryVector.getChildByOrdinal(1);

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BlockBuilder entryBuilder = builder.beginBlockEntry();
                int entryStart = vector.getElementStartIndex(i);
                int entryEnd = vector.getElementEndIndex(i);
                for (int entryIndex = entryStart; entryIndex < entryEnd; entryIndex++) {
                    assignBlockFromValueVector(keyVector, mapType.getKeyType(), entryBuilder, entryIndex, entryIndex + 1);
                    assignBlockFromValueVector(valueVector, mapType.getValueType(), entryBuilder, entryIndex, entryIndex + 1);
                }
                builder.closeEntry();
            }
        }
    }

    public void assignBlockFromStructVector(StructVector vector, Type type, BlockBuilder builder, int startIndex, int endIndex)
    {
        if (!(type instanceof RowType)) {
            throw new IllegalArgumentException("Type must be a RowType for StructVector");
        }

        RowType rowType = (RowType) type;

        for (int i = startIndex; i < endIndex; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BlockBuilder childBuilder = builder.beginBlockEntry();
                List<Type> childTypes = rowType.getTypeParameters();
                for (int childIndex = 0; childIndex < childTypes.size(); childIndex++) {
                    Type childType = childTypes.get(childIndex);
                    ValueVector childVector = vector.getChildByOrdinal(childIndex);
                    assignBlockFromValueVector(childVector, childType, childBuilder, i, i + 1);
                }
                builder.closeEntry();
            }
        }
    }
}
