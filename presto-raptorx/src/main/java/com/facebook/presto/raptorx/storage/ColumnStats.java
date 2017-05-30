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
package com.facebook.presto.raptorx.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonDeserialize(using = ColumnStats.Deserializer.class)
public class ColumnStats
{
    private final long columnId;
    private final Object min;
    private final Object max;

    public ColumnStats(long columnId, Object min, Object max)
    {
        this.columnId = columnId;
        this.min = min;
        this.max = max;
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @Nullable
    @JsonProperty
    public Object getMin()
    {
        return min;
    }

    @Nullable
    @JsonProperty
    public Object getMax()
    {
        return max;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnId", columnId)
                .toString();
    }

    /**
     * ColumnStats min/max values are always either boolean, long or binary.
     * Binary values are serialized in JSON as base64. When Jackson deserializes
     * into an Object, it does not know that the JSON string is actually encoded
     * binary data and thus it deserializes as a String. This custom deserializer
     * handles this by always treating JSON strings as binary.
     */
    public static class Deserializer
            extends StdDeserializer<ColumnStats>
    {
        public Deserializer()
        {
            super(ColumnStats.class);
        }

        @Override
        public ColumnStats deserialize(JsonParser parser, DeserializationContext context)
                throws IOException
        {
            JsonNode node = parser.readValueAsTree();
            return new ColumnStats(
                    node.get("columnId").longValue(),
                    convert(node.get("min")),
                    convert(node.get("max")));
        }

        private static Object convert(JsonNode node)
                throws IOException
        {
            if (node == null) {
                return null;
            }
            if (node.isBoolean()) {
                return node.asBoolean();
            }
            if (node.isNumber()) {
                return node.asLong();
            }
            if (node.isTextual()) {
                return node.binaryValue();
            }
            throw new IOException("Invalid node type: " + node.getNodeType());
        }
    }
}
