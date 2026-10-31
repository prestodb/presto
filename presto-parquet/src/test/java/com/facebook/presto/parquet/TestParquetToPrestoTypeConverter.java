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
package com.facebook.presto.parquet;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetToPrestoTypeConverter.fromParquet;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestParquetToPrestoTypeConverter
{
    private static final TypeManager TYPE_MANAGER = new ParquetTestTypeManager();

    @Test
    public void testPrimitives()
    {
        assertEquals(convertPrimitive(PrimitiveTypeName.BOOLEAN), BOOLEAN);
        assertEquals(convertPrimitive(PrimitiveTypeName.INT32), INTEGER);
        assertEquals(convertPrimitive(PrimitiveTypeName.INT64), BIGINT);
        assertEquals(convertPrimitive(PrimitiveTypeName.INT96), TIMESTAMP);
        assertEquals(convertPrimitive(PrimitiveTypeName.FLOAT), REAL);
        assertEquals(convertPrimitive(PrimitiveTypeName.DOUBLE), DOUBLE);
        assertEquals(convertPrimitive(PrimitiveTypeName.BINARY), VARBINARY);
    }

    @Test
    public void testFixedLenByteArrayDefaultsToVarbinary()
    {
        PrimitiveType type = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16).named("c");
        assertEquals(fromParquet(type, TYPE_MANAGER), VARBINARY);
    }

    @Test
    public void testString()
    {
        PrimitiveType utf8 = Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("c");
        assertEquals(fromParquet(utf8, TYPE_MANAGER), VARCHAR);
    }

    @Test
    public void testEnumAndJsonAreVarchar()
    {
        PrimitiveType enumType = Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.enumType()).named("c");
        PrimitiveType jsonType = Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.jsonType()).named("c");
        assertEquals(fromParquet(enumType, TYPE_MANAGER), VARCHAR);
        assertEquals(fromParquet(jsonType, TYPE_MANAGER), VARCHAR);
    }

    @Test
    public void testDate()
    {
        PrimitiveType type = Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("c");
        assertEquals(fromParquet(type, TYPE_MANAGER), DATE);
    }

    @Test
    public void testTimestampLocalVariants()
    {
        PrimitiveType millis = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("c");
        PrimitiveType micros = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named("c");
        assertEquals(fromParquet(millis, TYPE_MANAGER), TIMESTAMP);
        assertEquals(fromParquet(micros, TYPE_MANAGER), TIMESTAMP);
    }

    @Test
    public void testTimestampUtcAdjustedMapsToWithTimeZone()
    {
        PrimitiveType millisUtc = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("c");
        PrimitiveType microsUtc = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("c");
        assertEquals(fromParquet(millisUtc, TYPE_MANAGER), TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(fromParquet(microsUtc, TYPE_MANAGER), TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testTimestampNanosRejected()
    {
        PrimitiveType nanos = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS)).named("c");
        assertThrows(IllegalArgumentException.class, () -> fromParquet(nanos, TYPE_MANAGER));
    }

    @Test
    public void testTime()
    {
        PrimitiveType timeMillis = Types.optional(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("c");
        PrimitiveType timeMicros = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS)).named("c");
        assertEquals(fromParquet(timeMillis, TYPE_MANAGER), TIME);
        assertEquals(fromParquet(timeMicros, TYPE_MANAGER), TIME);
    }

    @Test
    public void testTimeUtcAdjustedMapsToWithTimeZone()
    {
        PrimitiveType timeMillisUtc = Types.optional(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("c");
        assertEquals(fromParquet(timeMillisUtc, TYPE_MANAGER), TIME_WITH_TIME_ZONE);
    }

    @Test
    public void testTimeNanosRejected()
    {
        PrimitiveType timeNanos = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.NANOS)).named("c");
        assertThrows(IllegalArgumentException.class, () -> fromParquet(timeNanos, TYPE_MANAGER));
    }

    @Test
    public void testDecimalShortAndLong()
    {
        PrimitiveType shortDecimal = Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.decimalType(4, 18)).named("c");
        PrimitiveType longDecimal = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(16)
                .as(LogicalTypeAnnotation.decimalType(10, 38)).named("c");
        assertEquals(fromParquet(shortDecimal, TYPE_MANAGER), DecimalType.createDecimalType(18, 4));
        assertEquals(fromParquet(longDecimal, TYPE_MANAGER), DecimalType.createDecimalType(38, 10));
    }

    @Test
    public void testSignedInt()
    {
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 8, true), TYPE_MANAGER), TINYINT);
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 16, true), TYPE_MANAGER), SMALLINT);
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 32, true), TYPE_MANAGER), INTEGER);
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT64, 64, true), TYPE_MANAGER), BIGINT);
    }

    @Test
    public void testUnsignedIntWidens()
    {
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 8, false), TYPE_MANAGER), SMALLINT);
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 16, false), TYPE_MANAGER), INTEGER);
        assertEquals(fromParquet(intAnnotated(PrimitiveTypeName.INT32, 32, false), TYPE_MANAGER), BIGINT);
    }

    @Test
    public void testUnsignedInt64Unsupported()
    {
        PrimitiveType type = intAnnotated(PrimitiveTypeName.INT64, 64, false);
        assertThrows(IllegalArgumentException.class, () -> fromParquet(type, TYPE_MANAGER));
    }

    @Test
    public void testUuid()
    {
        PrimitiveType type = Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(16)
                .as(LogicalTypeAnnotation.uuidType()).named("c");
        assertEquals(fromParquet(type, TYPE_MANAGER), UUID);
    }

    @Test
    public void testRow()
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalGroup()
                        .addField(Types.required(PrimitiveTypeName.INT64).named("a"))
                        .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b"))
                        .named("payload"))
                .named("root");

        Type converted = fromParquet(schema.getType("payload"), TYPE_MANAGER);
        Type expected = RowType.from(ImmutableList.of(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR)));
        assertEquals(converted, expected);
    }

    @Test
    public void testArrayOfPrimitive()
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalList()
                        .element(Types.required(PrimitiveTypeName.INT64).named("element"))
                        .named("arr"))
                .named("root");

        Type converted = fromParquet(schema.getType("arr"), TYPE_MANAGER);
        assertEquals(converted, new ArrayType(BIGINT));
    }

    @Test
    public void testArrayOfRow()
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalList()
                        .element(Types.optionalGroup()
                                .addField(Types.required(PrimitiveTypeName.INT64).named("a"))
                                .addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("b"))
                                .named("element"))
                        .named("arr_row"))
                .named("root");

        Type converted = fromParquet(schema.getType("arr_row"), TYPE_MANAGER);
        Type expected = new ArrayType(RowType.from(ImmutableList.of(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR))));
        assertEquals(converted, expected);
    }

    @Test
    public void testMap()
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalMap()
                        .key(Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("key"))
                        .value(Types.optional(PrimitiveTypeName.INT64).named("value"))
                        .named("m"))
                .named("root");

        Type converted = fromParquet(schema.getType("m"), TYPE_MANAGER);
        assertEquals(converted, TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(VARCHAR.getTypeSignature()),
                        TypeSignatureParameter.of(BIGINT.getTypeSignature()))));
    }

    @Test
    public void testLegacyTwoLevelList()
    {
        MessageType schema = Types.buildMessage()
                .addField(Types.optionalGroup()
                        .as(LogicalTypeAnnotation.listType())
                        .addField(new PrimitiveType(REPEATED, PrimitiveTypeName.INT64, "element"))
                        .named("legacy_list"))
                .named("root");

        Type converted = fromParquet(schema.getType("legacy_list"), TYPE_MANAGER);
        assertEquals(converted, new ArrayType(BIGINT));
    }

    private static Type convertPrimitive(PrimitiveTypeName name)
    {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(name);
        if (name == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            builder = builder.length(16);
        }
        return fromParquet(builder.named("c"), TYPE_MANAGER);
    }

    private static PrimitiveType intAnnotated(PrimitiveTypeName backing, int bitWidth, boolean signed)
    {
        return Types.optional(backing).as(LogicalTypeAnnotation.intType(bitWidth, signed)).named("c");
    }

    /**
     * Minimal {@link TypeManager} resolving the parameterized types the converter actually emits.
     */
    private static final class ParquetTestTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addParametricType(ParametricType parametricType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addType(Type type)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            if (StandardTypes.MAP.equals(baseTypeName)) {
                Type keyType = typeParameters.get(0).getTypeSignatureOrNamedTypeSignature().map(this::resolve).orElseThrow(IllegalArgumentException::new);
                Type valueType = typeParameters.get(1).getTypeSignatureOrNamedTypeSignature().map(this::resolve).orElseThrow(IllegalArgumentException::new);
                return new MapType(keyType, valueType, nativeValueGetter(keyType), nativeValueGetter(valueType));
            }
            throw new UnsupportedOperationException("Unsupported parameterized type in test: " + baseTypeName);
        }

        private Type resolve(TypeSignature signature)
        {
            String base = signature.getBase();
            switch (base) {
                case StandardTypes.BIGINT:
                    return BIGINT;
                case StandardTypes.INTEGER:
                    return INTEGER;
                case StandardTypes.SMALLINT:
                    return SMALLINT;
                case StandardTypes.TINYINT:
                    return TINYINT;
                case StandardTypes.DOUBLE:
                    return DOUBLE;
                case StandardTypes.REAL:
                    return REAL;
                case StandardTypes.BOOLEAN:
                    return BOOLEAN;
                case StandardTypes.DATE:
                    return DATE;
                case StandardTypes.TIME:
                    return TIME;
                case StandardTypes.TIMESTAMP:
                    return TIMESTAMP;
                case StandardTypes.VARCHAR:
                    return VARCHAR;
                case StandardTypes.VARBINARY:
                    return VARBINARY;
                case StandardTypes.UUID:
                    return UUID;
                default:
                    throw new UnsupportedOperationException("Unsupported base type in test: " + base);
            }
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasType(TypeSignature signature)
        {
            throw new UnsupportedOperationException();
        }
    }
}
