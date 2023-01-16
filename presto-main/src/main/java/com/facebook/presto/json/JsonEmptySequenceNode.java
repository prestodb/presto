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
package com.facebook.presto.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.io.IOException;
import java.util.List;

public class JsonEmptySequenceNode
        extends JsonNode
{
    public static final JsonEmptySequenceNode EMPTY_SEQUENCE = new JsonEmptySequenceNode();

    private JsonEmptySequenceNode() {}

    @Override
    public <T extends JsonNode> T deepCopy()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonToken asToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonParser.NumberType numberType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode get(int index)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode path(String fieldName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode path(int index)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonParser traverse()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonParser traverse(ObjectCodec codec)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected JsonNode _at(JsonPointer ptr)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNodeType getNodeType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String asText()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode findValue(String fieldName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode findPath(String fieldName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonNode findParent(String fieldName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> findValuesAsText(String fieldName, List<String> foundSoFar)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "EMPTY_SEQUENCE";
    }

    @Override
    public boolean equals(Object o)
    {
        return o == this;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
