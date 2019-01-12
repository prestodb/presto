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
package io.prestosql.plugin.kudu.properties;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RangeBoundValueDeserializer
        extends JsonDeserializer
{
    @Override
    public RangeBoundValue deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        JsonNode node = jp.getCodec().readTree(jp);

        if (node.isNull()) {
            return null;
        }
        else {
            List<Object> list;
            if (node.isArray()) {
                list = new ArrayList<>();
                Iterator<JsonNode> iter = node.elements();
                while (iter.hasNext()) {
                    Object v = toValue(iter.next());
                    list.add(v);
                }
            }
            else {
                Object v = toValue(node);
                list = ImmutableList.of(v);
            }
            return new RangeBoundValue(list);
        }
    }

    private Object toValue(JsonNode node)
            throws IOException
    {
        if (node.isTextual()) {
            return node.asText();
        }
        else if (node.isNumber()) {
            return node.numberValue();
        }
        else if (node.isBoolean()) {
            return node.asBoolean();
        }
        else if (node.isBinary()) {
            return node.binaryValue();
        }
        else {
            throw new IllegalStateException("Unexpected range bound value: " + node);
        }
    }
}
