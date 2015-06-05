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
package com.facebook.presto.kinesis.decoder.json;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.facebook.presto.kinesis.decoder.KinesisRowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.inject.Inject;

public class JsonKinesisRowDecoder
        implements KinesisRowDecoder
{
    public static final String NAME = "json";

    private final ObjectMapper objectMapper;

    @Inject
    public JsonKinesisRowDecoder(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<KinesisFieldValueProvider> fieldValueProviders, List<KinesisColumnHandle> columnHandles, Map<KinesisColumnHandle, KinesisFieldDecoder<?>> fieldDecoders)
    {
        JsonNode tree;

        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            return false;
        }

        for (KinesisColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            KinesisFieldDecoder<JsonNode> decoder = (KinesisFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                JsonNode node = locateNode(tree, columnHandle);
                fieldValueProviders.add(decoder.decode(node, columnHandle));
            }
        }
        return true;
    }

    private static JsonNode locateNode(JsonNode tree, KinesisColumnHandle columnHandle)
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
