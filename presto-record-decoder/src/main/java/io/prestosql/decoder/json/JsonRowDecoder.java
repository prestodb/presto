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
package io.prestosql.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * JSON specific row decoder.
 */
public class JsonRowDecoder
        implements RowDecoder
{
    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    JsonRowDecoder(ObjectMapper objectMapper, Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.fieldDecoders = ImmutableMap.copyOf(fieldDecoders);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data,
            Map<String, String> dataMap)
    {
        JsonNode tree;
        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            return Optional.empty();
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();

        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }

        return Optional.of(decodedRow);
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}
