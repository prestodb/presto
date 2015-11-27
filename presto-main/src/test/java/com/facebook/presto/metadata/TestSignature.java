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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestSignature
{
    @Test
    public void testRoundTrip()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        JsonCodec<Signature> codec = new JsonCodecFactory(objectMapperProvider, true).jsonCodec(Signature.class);

        Signature expected = new Signature("function", SCALAR, StandardTypes.BIGINT, ImmutableList.of(StandardTypes.BOOLEAN, StandardTypes.DOUBLE, StandardTypes.VARCHAR));

        String json = codec.toJson(expected);
        Signature actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getKind(), expected.getKind());
        assertEquals(actual.getReturnType(), expected.getReturnType());
        assertEquals(actual.getArgumentTypes(), expected.getArgumentTypes());
    }

    @Test
    public void testBasic()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("T"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.<Type>of(BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>"))), true, typeManager));
    }

    @Test
    public void testNonParametric()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.<TypeParameterRequirement>of(), "boolean", ImmutableList.of("bigint"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>"))), true, typeManager));
    }

    @Test
    public void testArray()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("get", SCALAR, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("array<T>"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>"))), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(BIGINT), true, typeManager));

        signature = new Signature("contains", SCALAR, ImmutableList.of(comparableTypeParameter("T")), "T", ImmutableList.of("array<T>", "T"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>")), BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>")), VARCHAR), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<HyperLogLog>")), HYPER_LOG_LOG), true, typeManager));

        signature = new Signature("foo", SCALAR, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("array<T>", "array<T>"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>")), typeManager.getType(parseTypeSignature("array<bigint>"))), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("array<bigint>")), typeManager.getType(parseTypeSignature("array<varchar>"))), true, typeManager));
    }

    @Test
    public void testMap()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("get", SCALAR, ImmutableList.of(typeParameter("K"), typeParameter("V")), "V", ImmutableList.of("map<K,V>", "K"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("map<bigint,varchar>")), BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(typeManager.getType(parseTypeSignature("map<bigint,varchar>")), VARCHAR), true, typeManager));
    }

    @Test
    public void testVariadic()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Type mapType = typeManager.getType(parseTypeSignature("map<bigint,bigint>"));
        Type arrayType = typeManager.getType(parseTypeSignature("array<bigint>"));
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(withVariadicBound("T", "map")), "bigint", ImmutableList.of("T"), true);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(mapType), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(arrayType), true, typeManager));
    }

    @Test
    public void testVarArgs()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeParameter("T")), "boolean", ImmutableList.of("T"), true);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(BIGINT, BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
    }

    @Test
    public void testCoercion()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeParameter("T")), "boolean", ImmutableList.of("T", "double"), true);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(DOUBLE, DOUBLE), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(BIGINT, BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));
    }

    @Test
    public void testUnknownCoercion()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeParameter("T")), "boolean", ImmutableList.of("T", "T"), false);
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(UNKNOWN, UNKNOWN), true, typeManager));
        assertNotNull(signature.bindTypeParameters(ImmutableList.of(UNKNOWN, BIGINT), true, typeManager));
        assertNull(signature.bindTypeParameters(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));

        signature = new Signature("foo", SCALAR, ImmutableList.of(comparableTypeParameter("T")), "boolean", ImmutableList.of("T", "T"), false);
        Map<String, Type> boundParameters = signature.bindTypeParameters(ImmutableList.of(UNKNOWN, BIGINT), true, typeManager);
        assertNotNull(boundParameters);
        assertEquals(boundParameters.get("T"), BIGINT);
        assertNull(signature.bindTypeParameters(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));
    }
}
