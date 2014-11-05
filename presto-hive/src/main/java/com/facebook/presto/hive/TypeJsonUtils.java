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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkState;

public final class TypeJsonUtils
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);

    private TypeJsonUtils() {}

    public static Object stackRepresentationToObject(ConnectorSession session, String value, Type type)
    {
        if (value == null) {
            return null;
        }

        Slice slice = Slices.utf8Slice(value);
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(slice.getInput())) {
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

        if (isArrayType(type)) {
            List<Object> list = new ArrayList<>();
            checkState(parser.getCurrentToken() == JsonToken.START_ARRAY, "Expected a json array");
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                list.add(stackRepresentationToObjectHelper(session, parser, type.getTypeParameters().get(0)));
            }

            return Collections.unmodifiableList(list);
        }

        if (isMapType(type)) {
            Map<Object, Object> map = new LinkedHashMap<>();
            checkState(parser.getCurrentToken() == JsonToken.START_OBJECT, "Expected a json object");
            while (parser.nextValue() != JsonToken.END_OBJECT) {
                Object key = mapKeyToObject(session, parser.getCurrentName(), type.getTypeParameters().get(0));
                Object value = stackRepresentationToObjectHelper(session, parser, type.getTypeParameters().get(1));
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
            type.writeDouble(blockBuilder, getDoubleValue(parser));
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(blockBuilder, Slices.utf8Slice(parser.getValueAsString()));
        }

        Object value = type.getObjectValue(session, blockBuilder.build(), 0);
        if (type.equals(DateType.DATE)) {
            return toJavaSqlDate((SqlDate) value);
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            return new Timestamp(((SqlTimestamp) value).getMillisUtc());
        }

        return value;
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

        Object value = type.getObjectValue(session, blockBuilder.build(), 0);
        if (type.equals(DateType.DATE)) {
            return toJavaSqlDate((SqlDate) value);
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            return new Timestamp(((SqlTimestamp) value).getMillisUtc());
        }

        return value;
    }

    private static Date toJavaSqlDate(SqlDate value)
    {
        int days = value.getDays();
        // todo should this be adjusted to midnight in JVM timezone?
        return new Date(TimeUnit.DAYS.toMillis(days));
    }

    public static double getDoubleValue(JsonParser parser) throws IOException
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
}
