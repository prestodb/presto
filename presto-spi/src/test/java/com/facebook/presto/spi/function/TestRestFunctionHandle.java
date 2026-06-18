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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.QualifiedObjectName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestRestFunctionHandle
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void testCreateWithExecutionEndpoint()
    {
        SqlFunctionId functionId = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function"),
                ImmutableList.of(parseTypeSignature("bigint")));
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        RestFunctionHandle handle = new RestFunctionHandle(
                functionId,
                "1.0",
                signature,
                Optional.of(URI.create("https://compute-cluster-1.example.com")));

        assertNotNull(handle.getExecutionEndpoint());
        assertTrue(handle.getExecutionEndpoint().isPresent());
        assertEquals(handle.getExecutionEndpoint().get().toString(), "https://compute-cluster-1.example.com");
    }

    @Test
    public void testCreateWithoutExecutionEndpoint()
    {
        SqlFunctionId functionId = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function"),
                ImmutableList.of(parseTypeSignature("bigint")));
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        RestFunctionHandle handle = new RestFunctionHandle(
                functionId,
                "1.0",
                signature,
                Optional.empty());

        assertNotNull(handle.getExecutionEndpoint());
        assertFalse(handle.getExecutionEndpoint().isPresent());
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        SqlFunctionId functionId = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function"),
                ImmutableList.of(parseTypeSignature("bigint")));
        // Use different types to make it obvious if they get swapped
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("varchar"),  // return type is varchar
                ImmutableList.of(parseTypeSignature("bigint")));  // arg type is bigint

        RestFunctionHandle original = new RestFunctionHandle(
                functionId,
                "1.0",
                signature,
                Optional.of(URI.create("https://compute-cluster-1.example.com")));

        String json = OBJECT_MAPPER.writeValueAsString(original);
        RestFunctionHandle deserialized = OBJECT_MAPPER.readValue(json, RestFunctionHandle.class);

        assertEquals(deserialized.getFunctionId(), original.getFunctionId());
        assertEquals(deserialized.getVersion(), original.getVersion());
        assertEquals(deserialized.getSignature().getReturnType(), original.getSignature().getReturnType());
        assertEquals(deserialized.getSignature().getArgumentTypes(), original.getSignature().getArgumentTypes());
        assertEquals(deserialized.getSignature(), original.getSignature());
        assertEquals(deserialized.getExecutionEndpoint(), original.getExecutionEndpoint());
    }

    @Test
    public void testSignatureSerialization()
            throws Exception
    {
        // Test Signature serialization independently
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("varchar"),  // return type
                ImmutableList.of(parseTypeSignature("bigint"), parseTypeSignature("double")));  // arg types

        String json = OBJECT_MAPPER.writeValueAsString(signature);
        Signature deserialized = OBJECT_MAPPER.readValue(json, Signature.class);

        assertEquals(deserialized.getName(), signature.getName());
        assertEquals(deserialized.getKind(), signature.getKind());
        assertEquals(deserialized.getReturnType(), signature.getReturnType());
        assertEquals(deserialized.getArgumentTypes(), signature.getArgumentTypes());
        assertEquals(deserialized, signature);
    }
}
