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
package com.facebook.presto.util;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.UnknownType;
import com.facebook.presto.type.VarcharOperators;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.DateTimeUtils.printDate;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.JsonUtil.ObjectKeyProvider.createObjectKeyProvider;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public final class JsonUtil
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    // This object mapper is constructed without .configure(ORDER_MAP_ENTRIES_BY_KEYS, true) because
    // `OBJECT_MAPPER.writeValueAsString(parser.readValueAsTree());` preserves input order.
    // Be aware. Using it arbitrarily can produce invalid json (ordered by key is required in Presto).
    private static final ObjectMapper OBJECT_MAPPED_UNORDERED = new ObjectMapper(JSON_FACTORY);

    private static final int MAX_JSON_LENGTH_IN_ERROR_MESSAGE = 10_000;

    private JsonUtil() {}

    public static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        return factory.createParser((InputStream) json.getInput());
    }

    public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output)
            throws IOException
    {
        return factory.createGenerator((OutputStream) output);
    }

    public static String truncateIfNecessaryForErrorMessage(Slice json)
    {
        if (json.length() <= MAX_JSON_LENGTH_IN_ERROR_MESSAGE) {
            return json.toStringUtf8();
        }
        else {
            return json.slice(0, MAX_JSON_LENGTH_IN_ERROR_MESSAGE).toStringUtf8() + "...(truncated)";
        }
    }

    public static boolean canCastToJson(Type type)
    {
        String baseType = type.getTypeSignature().getBase();
        if (baseType.equals(UnknownType.NAME) ||
                baseType.equals(StandardTypes.BOOLEAN) ||
                baseType.equals(StandardTypes.TINYINT) ||
                baseType.equals(StandardTypes.SMALLINT) ||
                baseType.equals(StandardTypes.INTEGER) ||
                baseType.equals(StandardTypes.BIGINT) ||
                baseType.equals(StandardTypes.REAL) ||
                baseType.equals(StandardTypes.DOUBLE) ||
                baseType.equals(StandardTypes.DECIMAL) ||
                baseType.equals(StandardTypes.VARCHAR) ||
                baseType.equals(StandardTypes.JSON) ||
                baseType.equals(StandardTypes.TIMESTAMP) ||
                baseType.equals(StandardTypes.DATE)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return canCastToJson(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return isValidJsonObjectKeyType(((MapType) type).getKeyType()) && canCastToJson(((MapType) type).getValueType());
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(JsonUtil::canCastToJson);
        }
        return false;
    }

    private static boolean isValidJsonObjectKeyType(Type type)
    {
        String baseType = type.getTypeSignature().getBase();
        return baseType.equals(UnknownType.NAME) ||
                baseType.equals(StandardTypes.BOOLEAN) ||
                baseType.equals(StandardTypes.TINYINT) ||
                baseType.equals(StandardTypes.SMALLINT) ||
                baseType.equals(StandardTypes.INTEGER) ||
                baseType.equals(StandardTypes.BIGINT) ||
                baseType.equals(StandardTypes.REAL) ||
                baseType.equals(StandardTypes.DOUBLE) ||
                baseType.equals(StandardTypes.DECIMAL) ||
                baseType.equals(StandardTypes.VARCHAR);
    }

    // transform the map key into string for use as JSON object key
    public interface ObjectKeyProvider
    {
        String getObjectKey(Block block, int position);

        static ObjectKeyProvider createObjectKeyProvider(Type type)
        {
            String baseType = type.getTypeSignature().getBase();
            switch (baseType) {
                case UnknownType.NAME:
                    return (block, position) -> null;
                case StandardTypes.BOOLEAN:
                    return (block, position) -> type.getBoolean(block, position) ? "true" : "false";
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    return (block, position) -> String.valueOf(type.getLong(block, position));
                case StandardTypes.REAL:
                    return (block, position) -> String.valueOf(intBitsToFloat((int) type.getLong(block, position)));
                case StandardTypes.DOUBLE:
                    return (block, position) -> String.valueOf(type.getDouble(block, position));
                case StandardTypes.DECIMAL:
                    DecimalType decimalType = (DecimalType) type;
                    if (isShortDecimal(decimalType)) {
                        return (block, position) -> Decimals.toString(decimalType.getLong(block, position), decimalType.getScale());
                    }
                    else {
                        return (block, position) -> Decimals.toString(
                                decodeUnscaledValue(type.getSlice(block, position)),
                                decimalType.getScale());
                    }
                case StandardTypes.VARCHAR:
                    return (block, position) -> type.getSlice(block, position).toStringUtf8();
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
            }
        }
    }

    // given block and position, write to JsonGenerator
    public interface JsonGeneratorWriter
    {
        // write a Json value into the JsonGenerator, provided by block and position
        void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException;

        static JsonGeneratorWriter createJsonGeneratorWriter(Type type)
        {
            String baseType = type.getTypeSignature().getBase();
            switch (baseType) {
                case UnknownType.NAME:
                    return new UnknownJsonGeneratorWriter();
                case StandardTypes.BOOLEAN:
                    return new BooleanJsonGeneratorWriter();
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    return new LongJsonGeneratorWriter(type);
                case StandardTypes.REAL:
                    return new RealJsonGeneratorWriter();
                case StandardTypes.DOUBLE:
                    return new DoubleJsonGeneratorWriter();
                case StandardTypes.DECIMAL:
                    if (isShortDecimal(type)) {
                        return new ShortDecimalJsonGeneratorWriter((DecimalType) type);
                    }
                    else {
                        return new LongDeicmalJsonGeneratorWriter((DecimalType) type);
                    }
                case StandardTypes.VARCHAR:
                    return new VarcharJsonGeneratorWriter(type);
                case StandardTypes.JSON:
                    return new JsonJsonGeneratorWriter();
                case StandardTypes.TIMESTAMP:
                    return new TimestampJsonGeneratorWriter();
                case StandardTypes.DATE:
                    return new DateGeneratorWriter();
                case StandardTypes.ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    return new ArrayJsonGeneratorWriter(
                            arrayType,
                            createJsonGeneratorWriter(arrayType.getElementType()));
                case StandardTypes.MAP:
                    MapType mapType = (MapType) type;
                    return new MapJsonGeneratorWriter(
                            mapType,
                            createObjectKeyProvider(mapType.getKeyType()),
                            createJsonGeneratorWriter(mapType.getValueType()));
                case StandardTypes.ROW:
                    List<Type> fieldTypes = type.getTypeParameters();
                    List<JsonGeneratorWriter> fieldWriters = new ArrayList<>(fieldTypes.size());
                    for (int i = 0; i < fieldTypes.size(); i++) {
                        fieldWriters.add(createJsonGeneratorWriter(fieldTypes.get(i)));
                    }
                    return new RowJsonGeneratorWriter((RowType) type, fieldWriters);
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
            }
        }
    }

    private static class UnknownJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            jsonGenerator.writeNull();
        }
    }

    private static class BooleanJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                boolean value = BOOLEAN.getBoolean(block, position);
                jsonGenerator.writeBoolean(value);
            }
        }
    }

    private static class LongJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final Type type;

        public LongJsonGeneratorWriter(Type type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                long value = type.getLong(block, position);
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class RealJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                float value = intBitsToFloat((int) REAL.getLong(block, position));
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class DoubleJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                double value = DOUBLE.getDouble(block, position);
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class ShortDecimalJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final DecimalType type;

        public ShortDecimalJsonGeneratorWriter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                BigDecimal value = BigDecimal.valueOf(type.getLong(block, position), type.getScale());
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class LongDeicmalJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final DecimalType type;

        public LongDeicmalJsonGeneratorWriter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                BigDecimal value = new BigDecimal(
                        decodeUnscaledValue(type.getSlice(block, position)),
                        type.getScale());
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class VarcharJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final Type type;

        public VarcharJsonGeneratorWriter(Type type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Slice value = type.getSlice(block, position);
                jsonGenerator.writeString(value.toStringUtf8());
            }
        }
    }

    private static class JsonJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Slice value = JSON.getSlice(block, position);
                jsonGenerator.writeRawValue(value.toStringUtf8());
            }
        }
    }

    private static class TimestampJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                long value = TIMESTAMP.getLong(block, position);
                jsonGenerator.writeString(printTimestampWithoutTimeZone(session.getTimeZoneKey(), value));
            }
        }
    }

    private static class DateGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                long value = DATE.getLong(block, position);
                jsonGenerator.writeString(printDate((int) value));
            }
        }
    }

    private static class ArrayJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final ArrayType type;
        private final JsonGeneratorWriter elementWriter;

        public ArrayJsonGeneratorWriter(ArrayType type, JsonGeneratorWriter elementWriter)
        {
            this.type = type;
            this.elementWriter = elementWriter;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Block arrayBlock = type.getObject(block, position);
                jsonGenerator.writeStartArray();
                for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                    elementWriter.writeJsonValue(jsonGenerator, arrayBlock, i, session);
                }
                jsonGenerator.writeEndArray();
            }
        }
    }

    private static class MapJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final MapType type;
        private final ObjectKeyProvider keyProvider;
        private final JsonGeneratorWriter valueWriter;

        public MapJsonGeneratorWriter(MapType type, ObjectKeyProvider keyProvider, JsonGeneratorWriter valueWriter)
        {
            this.type = type;
            this.keyProvider = keyProvider;
            this.valueWriter = valueWriter;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Block mapBlock = type.getObject(block, position);
                Map<String, Integer> orderedKeyToValuePosition = new TreeMap<>();
                for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                    String objectKey = keyProvider.getObjectKey(mapBlock, i);
                    orderedKeyToValuePosition.put(objectKey, i + 1);
                }

                jsonGenerator.writeStartObject();
                for (Map.Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
                    jsonGenerator.writeFieldName(entry.getKey());
                    valueWriter.writeJsonValue(jsonGenerator, mapBlock, entry.getValue(), session);
                }
                jsonGenerator.writeEndObject();
            }
        }
    }

    private static class RowJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final RowType type;
        private final List<JsonGeneratorWriter> fieldWriters;

        public RowJsonGeneratorWriter(RowType type, List<JsonGeneratorWriter> fieldWriters)
        {
            this.type = type;
            this.fieldWriters = fieldWriters;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position, ConnectorSession session)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Block rowBlock = type.getObject(block, position);
                jsonGenerator.writeStartArray();
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, rowBlock, i, session);
                }
                jsonGenerator.writeEndArray();
            }
        }
    }

    // utility classes and functions for cast from JSON
    public static Slice currentTokenAsVarchar(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return Slices.utf8Slice(parser.getText());
            case VALUE_NUMBER_FLOAT:
                // Avoidance of loss of precision does not seem to be possible here because of Jackson implementation.
                return DoubleOperators.castToVarchar(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                // An alternative is calling getLongValue and then BigintOperators.castToVarchar.
                // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
                return Slices.utf8Slice(parser.getText());
            case VALUE_TRUE:
                return BooleanOperators.castToVarchar(true);
            case VALUE_FALSE:
                return BooleanOperators.castToVarchar(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.VARCHAR, parser.getText()));
        }
    }

    public static Long currentTokenAsBigint(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToBigint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return DoubleOperators.castToLong(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return parser.getLongValue();
            case VALUE_TRUE:
                return BooleanOperators.castToBigint(true);
            case VALUE_FALSE:
                return BooleanOperators.castToBigint(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.BIGINT, parser.getText()));
        }
    }

    public static Long currentTokenAsInteger(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToInteger(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return DoubleOperators.castToInteger(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return (long) toIntExact(parser.getLongValue());
            case VALUE_TRUE:
                return BooleanOperators.castToInteger(true);
            case VALUE_FALSE:
                return BooleanOperators.castToInteger(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.INTEGER, parser.getText()));
        }
    }

    public static Long currentTokenAsSmallint(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToSmallint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return DoubleOperators.castToSmallint(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return (long) Shorts.checkedCast(parser.getLongValue());
            case VALUE_TRUE:
                return BooleanOperators.castToSmallint(true);
            case VALUE_FALSE:
                return BooleanOperators.castToSmallint(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.SMALLINT, parser.getText()));
        }
    }

    public static Long currentTokenAsTinyint(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToTinyint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return DoubleOperators.castToTinyint(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return (long) SignedBytes.checkedCast(parser.getLongValue());
            case VALUE_TRUE:
                return BooleanOperators.castToTinyint(true);
            case VALUE_FALSE:
                return BooleanOperators.castToTinyint(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.TINYINT, parser.getText()));
        }
    }

    public static Double currentTokenAsDouble(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToDouble(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return parser.getDoubleValue();
            case VALUE_NUMBER_INT:
                // An alternative is calling getLongValue and then BigintOperators.castToDouble.
                // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
                return parser.getDoubleValue();
            case VALUE_TRUE:
                return BooleanOperators.castToDouble(true);
            case VALUE_FALSE:
                return BooleanOperators.castToDouble(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.DOUBLE, parser.getText()));
        }
    }

    public static Long currentTokenAsReal(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToFloat(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return (long) floatToRawIntBits(parser.getFloatValue());
            case VALUE_NUMBER_INT:
                // An alternative is calling getLongValue and then BigintOperators.castToReal.
                // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
                return (long) floatToRawIntBits(parser.getFloatValue());
            case VALUE_TRUE:
                return BooleanOperators.castToReal(true);
            case VALUE_FALSE:
                return BooleanOperators.castToReal(false);
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.REAL, parser.getText()));
        }
    }

    public static Boolean currentTokenAsBoolean(JsonParser parser)
            throws IOException
    {
        switch (parser.currentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                return VarcharOperators.castToBoolean(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT:
                return DoubleOperators.castToBoolean(parser.getDoubleValue());
            case VALUE_NUMBER_INT:
                return BigintOperators.castToBoolean(parser.getLongValue());
            case VALUE_TRUE:
                return true;
            case VALUE_FALSE:
                return false;
            default:
                throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.BOOLEAN, parser.getText()));
        }
    }

    public static Long currentTokenAsShortDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal bigDecimal = currentTokenAsJavaDecimal(parser, precision, scale);
        return bigDecimal != null ? bigDecimal.unscaledValue().longValue() : null;
    }

    public static Slice currentTokenAsLongDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal bigDecimal = currentTokenAsJavaDecimal(parser, precision, scale);
        if (bigDecimal == null) {
            return null;
        }
        return encodeUnscaledValue(bigDecimal.unscaledValue());
    }

    // TODO: Instead of having BigDecimal as an intermediate step,
    // an alternative way is to make currentTokenAsShortDecimal and currentTokenAsLongDecimal
    // directly return the Long or Slice representation of the cast result
    // by calling the corresponding cast-to-decimal function, similar to other JSON cast function.
    private static BigDecimal currentTokenAsJavaDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal result;
        switch (parser.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
                result = new BigDecimal(parser.getText());
                result = result.setScale(scale, HALF_UP);
                break;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                result = parser.getDecimalValue();
                result = result.setScale(scale, HALF_UP);
                break;
            case VALUE_TRUE:
                result = BigDecimal.ONE.setScale(scale, HALF_UP);
                break;
            case VALUE_FALSE:
                result = BigDecimal.ZERO.setScale(scale, HALF_UP);
                break;
            default:
                throw new JsonCastException(format("Unexpected token when cast to DECIMAL(%s,%s): %s", precision, scale, parser.getText()));
        }

        if (result.precision() > precision) {
            // TODO: Should we use NUMERIC_VALUE_OUT_OF_RANGE instead?
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast input json to DECIMAL(%s,%s)", precision, scale));
        }
        return result;
    }

    // given a JSON parser, write to the BlockBuilder
    public interface BlockBuilderAppender
    {
        void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException;

        static BlockBuilderAppender createBlockBuilderAppender(Type type)
        {
            String baseType = type.getTypeSignature().getBase();
            switch (baseType) {
                case StandardTypes.BOOLEAN:
                    return new BooleanBlockBuilderAppender();
                case StandardTypes.TINYINT:
                    return new TinyintBlockBuilderAppender();
                case StandardTypes.SMALLINT:
                    return new SmallintBlockBuilderAppender();
                case StandardTypes.INTEGER:
                    return new IntegerBlockBuilderAppender();
                case StandardTypes.BIGINT:
                    return new BigintBlockBuilderAppender();
                case StandardTypes.REAL:
                    return new RealBlockBuilderAppender();
                case StandardTypes.DOUBLE:
                    return new DoubleBlockBuilderAppender();
                case StandardTypes.DECIMAL:
                    if (isShortDecimal(type)) {
                        return new ShortDecimalBlockBuilderAppender((DecimalType) type);
                    }
                    else {
                        return new LongDecimalBlockBuilderAppender((DecimalType) type);
                    }
                case StandardTypes.VARCHAR:
                    return new VarcharBlockBuilderAppender(type);
                case StandardTypes.JSON:
                    return (parser, blockBuilder) -> {
                        String json = OBJECT_MAPPED_UNORDERED.writeValueAsString(parser.readValueAsTree());
                        JSON.writeSlice(blockBuilder, Slices.utf8Slice(json));
                    };
                case StandardTypes.ARRAY:
                    return new ArrayBlockBuilderAppender(createBlockBuilderAppender(((ArrayType) type).getElementType()));
                case StandardTypes.MAP:
                    throw new UnsupportedOperationException();
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
            }
        }
    }

    private static class BooleanBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Boolean result = currentTokenAsBoolean(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(blockBuilder, result);
            }
        }
    }

    private static class TinyintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsTinyint(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                TINYINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class SmallintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsInteger(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                SMALLINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class IntegerBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsInteger(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                INTEGER.writeLong(blockBuilder, result);
            }
        }
    }

    private static class BigintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsBigint(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class RealBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsReal(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                REAL.writeLong(blockBuilder, result);
            }
        }
    }

    private static class DoubleBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Double result = currentTokenAsDouble(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, result);
            }
        }
    }

    private static class ShortDecimalBlockBuilderAppender
            implements BlockBuilderAppender
    {
        DecimalType type;

        ShortDecimalBlockBuilderAppender(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsShortDecimal(parser, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private static class LongDecimalBlockBuilderAppender
            implements BlockBuilderAppender
    {
        DecimalType type;

        LongDecimalBlockBuilderAppender(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Slice result = currentTokenAsLongDecimal(parser, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeSlice(blockBuilder, result);
            }
        }
    }

    private static class VarcharBlockBuilderAppender
            implements BlockBuilderAppender
    {
        Type type;

        VarcharBlockBuilderAppender(Type type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Slice result = currentTokenAsVarchar(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeSlice(blockBuilder, result);
            }
        }
    }

    private static class ArrayBlockBuilderAppender
            implements BlockBuilderAppender
    {
        BlockBuilderAppender elementAppender;

        ArrayBlockBuilderAppender(BlockBuilderAppender elementAppender)
        {
            this.elementAppender = elementAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            if (parser.getCurrentToken() != START_ARRAY) {
                throw new JsonCastException(format("Expected a json array, but got %s", parser.getText()));
            }
            BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
            while (parser.nextToken() != END_ARRAY) {
                elementAppender.append(parser, entryBuilder);
            }
            blockBuilder.closeEntry();
        }
    }
}
