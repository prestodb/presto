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
package com.facebook.presto.decoder.json;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * Default field decoder for the JSON format. Supports json format coercions to implicitly convert e.g. string to long values.
 */
public class DefaultJsonFieldDecoder
        implements JsonFieldDecoder
{
    private final DecoderColumnHandle columnHandle;

    public DefaultJsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!isSupportedType(columnHandle.getType())) {
            JsonRowDecoderFactory.throwUnsupportedColumnType(columnHandle);
        }
    }

    private boolean isSupportedType(Type type)
    {
        if (isVarcharType(type)) {
            return true;
        }
        if (ImmutableList.of(
                BIGINT,
                INTEGER,
                SMALLINT,
                TINYINT,
                BOOLEAN,
                DOUBLE
        ).contains(type)) {
            return true;
        }
        return false;
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new JsonValueProvider(value, columnHandle);
    }

    public static class JsonValueProvider
            extends FieldValueProvider
    {
        private final JsonNode value;
        private final DecoderColumnHandle columnHandle;

        public JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            this.value = value;
            this.columnHandle = columnHandle;
        }

        @Override
        public final boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public boolean getBoolean()
        {
            return value.asBoolean();
        }

        @Override
        public long getLong()
        {
            return value.asLong();
        }

        @Override
        public double getDouble()
        {
            return value.asDouble();
        }

        @Override
        public Slice getSlice()
        {
            String textValue = value.isValueNode() ? value.asText() : value.toString();
            Slice slice = utf8Slice(textValue);
            if (isVarcharType(columnHandle.getType())) {
                slice = truncateToLength(slice, columnHandle.getType());
            }
            return slice;
        }
    }
}
