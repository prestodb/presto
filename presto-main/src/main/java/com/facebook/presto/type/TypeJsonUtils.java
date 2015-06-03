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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class TypeJsonUtils
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private TypeJsonUtils() {}

    public static Object stackRepresentationToObject(ConnectorSession session, Slice value, Type type)
    {
        if (value == null) {
            return null;
        }

        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(value.getInput())) {
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
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, checkNotNull(sliceValue, "sliceValue is null").length());
        }

        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, parser.getBooleanValue());
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, parser.getLongValue());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, getDoubleValue(parser));
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, checkNotNull(sliceValue, "sliceValue is null"));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
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
        if (type.getJavaType() == boolean.class) {
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

    private static double getDoubleValue(JsonParser parser) throws IOException
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
                baseType.equals(StandardTypes.BIGINT) ||
                baseType.equals(StandardTypes.DOUBLE) ||
                baseType.equals(StandardTypes.VARCHAR)) {
            return true;
        }
        if (type instanceof ArrayType) {
            return canCastFromJson(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return canCastFromJson(((MapType) type).getKeyType()) && canCastFromJson(((MapType) type).getValueType());
        }
        return false;
    }
}
