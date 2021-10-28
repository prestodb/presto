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
package com.facebook.presto.common.type;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestParameterKind
{
    public static final JsonCodec<ParameterKind> PARAMETER_KIND_CODEC;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        PARAMETER_KIND_CODEC = codecFactory.jsonCodec(ParameterKind.class);
    }

    @Test
    public void testSerialize()
    {
        assertEquals(PARAMETER_KIND_CODEC.toJson(ParameterKind.TYPE), "\"TYPE_SIGNATURE\"");
        assertEquals(PARAMETER_KIND_CODEC.toJson(ParameterKind.NAMED_TYPE), "\"NAMED_TYPE_SIGNATURE\"");
        assertEquals(PARAMETER_KIND_CODEC.toJson(ParameterKind.LONG), "\"LONG_LITERAL\"");
        assertEquals(PARAMETER_KIND_CODEC.toJson(ParameterKind.VARIABLE), "\"VARIABLE\"");
    }

    @Test
    public void testDeserializeFromOldFormat()
    {
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"TYPE_SIGNATURE\""), ParameterKind.TYPE);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"NAMED_TYPE_SIGNATURE\""), ParameterKind.NAMED_TYPE);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"LONG_LITERAL\""), ParameterKind.LONG);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"VARIABLE\""), ParameterKind.VARIABLE);
    }

    @Test
    public void testDeserializeFromNewFormat()
    {
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"TYPE\""), ParameterKind.TYPE);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"NAMED_TYPE\""), ParameterKind.NAMED_TYPE);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"LONG\""), ParameterKind.LONG);
        assertEquals(PARAMETER_KIND_CODEC.fromJson("\"VARIABLE\""), ParameterKind.VARIABLE);
    }
}
