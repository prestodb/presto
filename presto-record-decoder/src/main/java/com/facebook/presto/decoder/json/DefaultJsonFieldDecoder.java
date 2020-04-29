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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Default field decoder for the JSON format. Supports json format coercions to implicitly convert e.g. string to long values.
 */
public class DefaultJsonFieldDecoder
        implements JsonFieldDecoder
{
    private final DecoderColumnHandle columnHandle;
    private final long minValue;
    private final long maxValue;

    public DefaultJsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!isSupportedType(columnHandle.getType())) {
            JsonRowDecoderFactory.throwUnsupportedColumnType(columnHandle);
        }

        if (columnHandle.getType() == TINYINT) {
            minValue = Byte.MIN_VALUE;
            maxValue = Byte.MAX_VALUE;
        }
        else if (columnHandle.getType() == SMALLINT) {
            minValue = Short.MIN_VALUE;
            maxValue = Short.MAX_VALUE;
        }
        else if (columnHandle.getType() == INTEGER) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        }
        else if (columnHandle.getType() == BIGINT) {
            minValue = Long.MIN_VALUE;
            maxValue = Long.MAX_VALUE;
        }
        else {
            // those values will not be used if column type is not one of mentioned above
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
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
        return new JsonValueProvider(value, columnHandle, minValue, maxValue);
    }

    public static class JsonValueProvider
            extends FieldValueProvider
    {
        private final JsonNode value;
        private final DecoderColumnHandle columnHandle;
        private final long minValue;
        private final long maxValue;

        public JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle, long minValue, long maxValue)
        {
            this.value = value;
            this.columnHandle = columnHandle;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        public final boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public boolean getBoolean()
        {
            if (value.isValueNode()) {
                return value.asBoolean();
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        public long getLong()
        {
            try {
                long longValue;
                if (value.isIntegralNumber() && !value.isBigInteger()) {
                    longValue = value.longValue();
                    if (longValue >= minValue && longValue <= maxValue) {
                        return longValue;
                    }
                }
                else if (value.isValueNode()) {
                    longValue = parseLong(value.asText());
                    if (longValue >= minValue && longValue <= maxValue) {
                        return longValue;
                    }
                }
            }
            catch (NumberFormatException ignore) {
                // ignore
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        public double getDouble()
        {
            try {
                if (value.isNumber()) {
                    return value.doubleValue();
                }
                if (value.isValueNode()) {
                    return parseDouble(value.asText());
                }
            }
            catch (NumberFormatException ignore) {
                // ignore
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
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
