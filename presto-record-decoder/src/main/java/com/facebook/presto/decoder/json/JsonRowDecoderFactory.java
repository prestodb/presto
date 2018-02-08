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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.decoder.RowDecoderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
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

    private Map<DecoderColumnHandle, FieldDecoder<JsonNode>> chooseFieldDecoders(Set<DecoderColumnHandle> columns)
    {
        return columns.stream()
                .collect(toImmutableMap(identity(), this::chooseFieldDecoder));
    }

    private FieldDecoder<JsonNode> chooseFieldDecoder(DecoderColumnHandle column)
    {
        checkArgument(!column.isInternal(), "unexpected internal column '%s'", column.getName());
        if (column.getDataFormat() == null || column.getDataFormat().equals(DEFAULT_FIELD_DECODER_NAME)) {
            return new JsonFieldDecoder();
        }
        Class<?> javaType = column.getType().getJavaType();
        if (javaType == Slice.class || javaType == long.class) {
            switch (column.getDataFormat()) {
                case "custom-date-time":
                    return new CustomDateTimeJsonFieldDecoder();
                case "iso8601":
                    return new ISO8601JsonFieldDecoder();
                case "seconds-since-epoch":
                    return new SecondsSinceEpochJsonFieldDecoder();
                case "milliseconds-since-epoch":
                    return new MillisecondsSinceEpochJsonFieldDecoder();
                case "rfc2822":
                    return new RFC2822JsonFieldDecoder();
            }
        }
        // fallback to default one
        throw new IllegalArgumentException(String.format("unknown data format '%s' for column '%s'", column.getName(), column.getDataFormat()));
    }
}
