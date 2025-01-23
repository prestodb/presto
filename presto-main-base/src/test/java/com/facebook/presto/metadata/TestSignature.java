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
package com.facebook.presto.metadata;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static org.testng.Assert.assertEquals;

public class TestSignature
{
    @Test
    public void testSerializationRoundTrip()
    {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        JsonCodec<Signature> codec = new JsonCodecFactory(objectMapperProvider, true).jsonCodec(Signature.class);

        Signature expected = new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "function"),
                SCALAR,
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.VARCHAR)));

        String json = codec.toJson(expected);
        Signature actual = codec.fromJson(json);

        assertEquals(actual.getNameSuffix(), expected.getNameSuffix());
        assertEquals(actual.getKind(), expected.getKind());
        assertEquals(actual.getReturnType(), expected.getReturnType());
        assertEquals(actual.getArgumentTypes(), expected.getArgumentTypes());
    }
}
