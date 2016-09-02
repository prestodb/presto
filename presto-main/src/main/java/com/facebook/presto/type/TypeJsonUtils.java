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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.RealType.REAL;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public final class TypeJsonUtils
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    // This object mapper is constructed without .configure(ORDER_MAP_ENTRIES_BY_KEYS, true) because
    // `OBJECT_MAPPER.writeValueAsString(parser.readValueAsTree());` preserves input order.
    // Be aware. Using it arbitrarily can produce invalid json (ordered by key is required in presto).
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY);

    private TypeJsonUtils() {}

    public static Object stackRepresentationToObject(ConnectorSession session, Slice value, Type type)
    {
        if (value == null) {
            return null;
        }

        try (JsonParser jsonParser = JSON_FACTORY.createParser((InputStream) value.getInput())) {
            jsonParser.nextToken();
            return stackRepresentationToObjectHelper(session, jsonParser, type);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Object stackRepresentationToObjectHelper(ConnectorSession session, JsonParser parser, Type type)
            throws IOException
    {
        // checking whether type is JsonType needs to go before null check because
        // cast('[null]', array(json)) should be casted to a single item array containing a json document "null" instead of sql null.
        if (type instanceof JsonType) {
            return OBJECT_MAPPER.writeValueAsString(parser.readValueAsTree());
        }

        if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
            return null;
        }

        if (type instanceof ArrayType) {
            List<Object> list = new ArrayList<>();
            checkState(parser.getCurrentToken() == JsonToken.START_ARRAY, "Expected a json array");
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                list.add(stackRepresentationToObjectHelper(session, parser, ((ArrayType) type).getElementType()));
            }

            return Collections.unmodifiableList(list);
        }

        if (type instanceof MapType) {
            Map<Object, Object> map = new LinkedHashMap<>();
            checkState(parser.getCurrentToken() == JsonToken.START_OBJECT, "Expected a json object");
            while (parser.nextValue() != JsonToken.END_OBJECT) {
                Object key = mapKeyToObject(session, parser.getCurrentName(), ((MapType) type).getKeyType());
                Object value = stackRepresentationToObjectHelper(session, parser, ((MapType) type).getValueType());
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }

        if (type instanceof RowType) {
            List<Object> list = new ArrayList<>();
            checkState(parser.getCurrentToken() == JsonToken.START_ARRAY, "Expected a json array");
            int field = 0;
            RowType rowType = (RowType) type;
            while (parser.nextValue() != JsonToken.END_ARRAY) {
                checkArgument(field < rowType.getFields().size(), "Unexpected field for type %s", type);
                Object value = stackRepresentationToObjectHelper(session, parser, rowType.getFields().get(field).getType());
                list.add(value);
                field++;
            }
            checkArgument(field == rowType.getFields().size(), "Expected %s fields for type %s", rowType.getFields().size(), type);

            return Collections.unmodifiableList(list);
        }

        Slice sliceValue = null;
        if (type.getJavaType() == Slice.class) {
            sliceValue = Slices.utf8Slice(parser.getValueAsString());
        }

        BlockBuilder blockBuilder;
        if (type instanceof FixedWidthType) {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        }
        else {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, requireNonNull(sliceValue, "sliceValue is null").length());
        }

        if (type instanceof DecimalType) {
            return getSqlDecimal((DecimalType) type, parser.getDecimalValue());
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, parser.getBooleanValue());
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, parser.getLongValue());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, getDoubleValue(parser));
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, requireNonNull(sliceValue, "sliceValue is null"));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
    }

    private static SqlDecimal getSqlDecimal(DecimalType decimalType, BigDecimal decimalValue)
    {
        BigInteger unscaledValue = decimalValue.setScale(decimalType.getScale(), RoundingMode.HALF_UP).unscaledValue();
        if (Decimals.overflows(unscaledValue, decimalType.getPrecision())) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, String.format("DECIMAL with unscaled value %s exceeds precision %s", unscaledValue, decimalType.getPrecision()));
        }
        return new SqlDecimal(unscaledValue,
                decimalType.getPrecision(),
                decimalType.getScale());
    }

    private static Object mapKeyToObject(ConnectorSession session, String jsonKey, Type type)
    {
        BlockBuilder blockBuilder;
        if (type instanceof FixedWidthType) {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        }
        else {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, jsonKey.length());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getSqlDecimal(decimalType, new BigDecimal(jsonKey));
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, Boolean.parseBoolean(jsonKey));
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, Long.parseLong(jsonKey));
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, Double.parseDouble(jsonKey));
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, Slices.utf8Slice(jsonKey));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
    }

    private static double getDoubleValue(JsonParser parser)
            throws IOException
    {
        double value;
        try {
            value = parser.getDoubleValue();
        }
        catch (JsonParseException e) {
            //handle non-numeric numbers (inf/nan)
            value = Double.parseDouble(parser.getValueAsString());
        }
        return value;
    }

    public static boolean canCastFromJson(Type type)
    {
        String baseType = type.getTypeSignature().getBase();
        if (baseType.equals(StandardTypes.BOOLEAN) ||
                baseType.equals(StandardTypes.TINYINT) ||
                baseType.equals(StandardTypes.SMALLINT) ||
                baseType.equals(StandardTypes.INTEGER) ||
                baseType.equals(StandardTypes.BIGINT) ||
                baseType.equals(StandardTypes.DOUBLE) ||
                baseType.equals(StandardTypes.VARCHAR) ||
                baseType.equals(StandardTypes.DECIMAL) ||
                baseType.equals(StandardTypes.JSON)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return canCastFromJson(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return isValidJsonObjectKeyType(((MapType) type).getKeyType()) && canCastFromJson(((MapType) type).getValueType());
        }
        return false;
    }

    private static boolean isValidJsonObjectKeyType(Type type)
    {
        String baseType = type.getTypeSignature().getBase();
        return baseType.equals(StandardTypes.BOOLEAN) ||
                baseType.equals(StandardTypes.TINYINT) ||
                baseType.equals(StandardTypes.SMALLINT) ||
                baseType.equals(StandardTypes.INTEGER) ||
                baseType.equals(StandardTypes.BIGINT) ||
                baseType.equals(StandardTypes.DOUBLE) ||
                baseType.equals(StandardTypes.DECIMAL) ||
                baseType.equals(StandardTypes.VARCHAR);
    }

    @VisibleForTesting
    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else if (element instanceof SqlDecimal) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) element).getUnscaledValue()));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            type.writeObject(blockBuilder, element);
        }
    }
}
