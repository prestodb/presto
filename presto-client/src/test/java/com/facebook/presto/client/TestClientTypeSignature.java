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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestClientTypeSignature
{
    public static final JsonCodec<ClientTypeSignature> CLIENT_TYPE_SIGNATURE_CODEC;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        CLIENT_TYPE_SIGNATURE_CODEC = codecFactory.jsonCodec(ClientTypeSignature.class);
    }

    @Test
    public void testJsonRoundTrip()
    {
        TypeSignature bigint = BIGINT.getTypeSignature();
        assertJsonRoundTrip(new ClientTypeSignature(bigint));
        assertJsonRoundTrip(new ClientTypeSignature(
                "array",
                ImmutableList.of(new ClientTypeSignatureParameter(TypeSignatureParameter.of(bigint)))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "foo",
                ImmutableList.of(new ClientTypeSignatureParameter(TypeSignatureParameter.of(42)))));
        assertJsonRoundTrip(new ClientTypeSignature(
                "row",
                ImmutableList.of(
                        new ClientTypeSignatureParameter(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("foo", false)), bigint))),
                        new ClientTypeSignatureParameter(TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("bar", false)), bigint))))));
    }

    @Test
    public void testBackwardsCompatible()
    {
        ClientTypeSignature signature = new ClientTypeSignature(StandardTypes.ARRAY, ImmutableList.of(new ClientTypeSignatureParameter(TypeSignatureParameter.of(BIGINT.getTypeSignature()))));
        ClientTypeSignature legacy = CLIENT_TYPE_SIGNATURE_CODEC.fromJson("{\"rawType\":\"array\",\"literalArguments\":[],\"typeArguments\":[{\"rawType\":\"bigint\",\"literalArguments\":[],\"typeArguments\":[]}]}");
        assertEquals(legacy, signature);
    }

    private static void assertJsonRoundTrip(ClientTypeSignature signature)
    {
        String json = CLIENT_TYPE_SIGNATURE_CODEC.toJson(signature);
        ClientTypeSignature copy = CLIENT_TYPE_SIGNATURE_CODEC.fromJson(json);
        assertEquals(copy, signature);
    }
}
