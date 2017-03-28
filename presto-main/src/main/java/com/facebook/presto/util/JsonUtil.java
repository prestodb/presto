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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.UnknownType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.DateTimeUtils.printDate;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.JsonUtil.ObjectKeyProvider.createObjectKeyProvider;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class JsonUtil
{
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
}
