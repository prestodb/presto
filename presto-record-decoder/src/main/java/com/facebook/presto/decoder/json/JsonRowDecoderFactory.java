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

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.decoder.RowDecoderFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class JsonRowDecoderFactory
        implements RowDecoderFactory
{
    private final ObjectMapper objectMapper;

    @Inject
    public JsonRowDecoderFactory(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    @Override
    public RowDecoder create(Map<String, String> decoderParams, Set<DecoderColumnHandle> columns)
    {
        requireNonNull(columns, "columnHandles is null");
        return new JsonRowDecoder(objectMapper, chooseFieldDecoders(columns));
    }

    private Map<DecoderColumnHandle, JsonFieldDecoder> chooseFieldDecoders(Set<DecoderColumnHandle> columns)
    {
        return columns.stream()
                .collect(toImmutableMap(identity(), this::chooseFieldDecoder));
    }

    private JsonFieldDecoder chooseFieldDecoder(DecoderColumnHandle column)
    {
        try {
            requireNonNull(column);
            checkArgument(!column.isInternal(), "unexpected internal column '%s'", column.getName());

            String dataFormat = Optional.ofNullable(column.getDataFormat()).orElse("");
            switch (dataFormat) {
                case "custom-date-time":
                    return new CustomDateTimeJsonFieldDecoder(column);
                case "iso8601":
                    return new ISO8601JsonFieldDecoder(column);
                case "seconds-since-epoch":
                    return new SecondsSinceEpochJsonFieldDecoder(column);
                case "milliseconds-since-epoch":
                    return new MillisecondsSinceEpochJsonFieldDecoder(column);
                case "rfc2822":
                    return new RFC2822JsonFieldDecoder(column);
                case "":
                    return new DefaultJsonFieldDecoder(column);
                default:
                    throw new IllegalArgumentException(format("unknown data format '%s' used for column '%s'", column.getDataFormat(), column.getName()));
            }
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(GENERIC_USER_ERROR, e);
        }
    }

    public static JsonFieldDecoder throwUnsupportedColumnType(DecoderColumnHandle column)
    {
        if (column.getDataFormat() == null) {
            throw new IllegalArgumentException(format("unsupported column type '%s' for column '%s'", column.getType().getDisplayName(), column.getName()));
        }
        throw new IllegalArgumentException(format("unsupported column type '%s' for column '%s' with data format '%s'", column.getType(), column.getName(), column.getDataFormat()));
    }
}
