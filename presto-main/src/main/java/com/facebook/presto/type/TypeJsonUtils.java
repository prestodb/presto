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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonFactory;
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

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, parser.getBooleanValue());
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, parser.getLongValue());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, parser.getDoubleValue());
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, Slices.utf8Slice(parser.getValueAsString()));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
    }

    private static Object mapKeyToObject(ConnectorSession session, String jsonKey, Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, Boolean.valueOf(jsonKey));
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, Long.valueOf(jsonKey));
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, Double.valueOf(jsonKey));
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, Slices.utf8Slice(jsonKey));
        }
        return type.getObjectValue(session, blockBuilder.build(), 0);
    }
}
