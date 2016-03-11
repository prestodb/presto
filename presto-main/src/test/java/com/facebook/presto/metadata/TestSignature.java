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

import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.DecimalParametricType.DECIMAL;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestSignature
{
    private final TypeSignature varcharX = new TypeSignature(StandardTypes.VARCHAR, ImmutableList.of(TypeSignatureParameter.of("x")));
    private final TypeSignature varcharY = new TypeSignature(StandardTypes.VARCHAR, ImmutableList.of(TypeSignatureParameter.of("y")));

    @Test
    public void testBindLiteralForDecimal()
    {
        // given a function with signature (decimal(p,s), decimal(p,s))->boolean,
        // and invocation with (decimal(2,1), decimal(1,0))
        // verify that p is bound to 2 an s is bound to 1

        TypeSignature booleanSignature = BooleanType.BOOLEAN.getTypeSignature();
        TypeSignature decimal = new TypeSignature(
                StandardTypes.DECIMAL,
                ImmutableList.of(TypeSignatureParameter.of("p"), TypeSignatureParameter.of("s")));

        TypeSignature decimal21 = DECIMAL.createType(ImmutableList.of(TypeParameter.of(2), TypeParameter.of(1))).getTypeSignature();
        TypeSignature decimal10 = DECIMAL.createType(ImmutableList.of(TypeParameter.of(1), TypeParameter.of(0))).getTypeSignature();

        Signature function = new Signature(
                "function",
                SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                booleanSignature,
                ImmutableList.of(decimal, decimal),
                false);

        assertEquals(
                function.bindLongVariables(ImmutableList.of(decimal21, decimal10)),
                ImmutableMap.of("P", OptionalLong.of(2), "S", OptionalLong.of(1)));

        assertEquals(
                function.bindLongVariables(ImmutableList.of(decimal10, decimal21)),
                ImmutableMap.of("P", OptionalLong.of(2), "S", OptionalLong.of(1)));
    }

    @Test
    public void testResolveCalculatedTypes()
    {
        // given function(varchar(x), varchar(y)):boolean
        Signature function = new Signature(
                "function",
                SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BooleanType.BOOLEAN.getTypeSignature(),
                ImmutableList.of(varcharX, varcharY),
                false);

        TypeSignature varchar42 = createVarcharType(42).getTypeSignature();
        TypeSignature varchar44 = createVarcharType(44).getTypeSignature();
        TypeSignature varchar = new TypeSignature(StandardTypes.VARCHAR, ImmutableList.of());
        assertEquals(
                function.resolveCalculatedTypes(ImmutableList.of(varchar42, varchar44)).getArgumentTypes(),
                ImmutableList.of(varchar42, varchar44));

        assertEquals(
                function.resolveCalculatedTypes(ImmutableList.of(UNKNOWN.getTypeSignature(), varchar44)).getArgumentTypes(),
                ImmutableList.of(varchar, varchar44));
    }

    @Test
    public void testBindUnknown()
    {
        // given function(varchar(x)):boolean
        // does it bind to argument UNKNOWN
        // without coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                BooleanType.BOOLEAN.getTypeSignature(),
                ImmutableList.of(varcharX),
                Optional.empty(),
                ImmutableList.of(UNKNOWN),
                false,
                null);
        // with coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                BooleanType.BOOLEAN.getTypeSignature(),
                ImmutableList.of(varcharX),
                Optional.empty(),
                ImmutableList.of(UNKNOWN),
                true,
                ImmutableMap.of());
    }

    @Test
    public void testBindUnknownToConcreteArray()
    {
        TypeSignature booleanSignature = BooleanType.BOOLEAN.getTypeSignature();
        // given function(array(boolean)):boolean
        // does it bind to argument UNKNOWN
        // with coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                booleanSignature,
                ImmutableList.of(new TypeSignature(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(booleanSignature)))),
                Optional.empty(),
                ImmutableList.of(UNKNOWN),
                true,
                ImmutableMap.of());
    }

    @Test
    public void testBindUnknownToArray()
    {
        TypeSignature templateType = new TypeSignature("T", ImmutableList.of());
        // given function(array(T)):T
        // does it bind to argument UNKNOWN
        // without coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                templateType,
                ImmutableList.of(new TypeSignature(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(templateType)))),
                Optional.empty(),
                ImmutableList.of(UNKNOWN),
                false,
                null);
        // with coercion
        // TODO: fix this
        /*
        assertFunctionBind(
                ImmutableList.of(),
                templateType,
                ImmutableList.of(new TypeSignature(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(templateType)))),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableList.of(UNKNOWN),
                true,
                ImmutableMap.of("T", UNKNOWN));*/
    }

    @Test
    public void testBindVarcharTemplateStyle()
    {
        TypeSignature templateType1 = new TypeSignature("T1", ImmutableList.of());
        TypeSignature templateType2 = new TypeSignature("T2", ImmutableList.of());
        Type varchar42 = createVarcharType(42);
        Type varchar1 = createVarcharType(1);

        // given f(T1):T2 bind f(varchar(42)):varchar(1)
        assertFunctionBind(
                ImmutableList.of(new TypeVariableConstraint("T1", true, false, "varchar"), new TypeVariableConstraint("T2", true, false, "varchar")),
                ImmutableList.of(),
                templateType2,
                ImmutableList.of(templateType1),
                Optional.of(varchar1),
                ImmutableList.of(varchar42),
                false,
                ImmutableMap.of("T1", varchar42, "T2", varchar1));
    }

    @Test
    public void testBindVarchar()
    {
        Type varchar44 = createVarcharType(44);
        Type varchar42 = createVarcharType(42);
        Type varchar1 = createVarcharType(1);

        // given f(varchar(42)):varchar(42) bind f(varchar(44)):varchar(44)
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                varchar42.getTypeSignature(),
                ImmutableList.of(varchar42.getTypeSignature()),
                Optional.of(varchar44),
                ImmutableList.of(varchar44),
                true,
                null);

        // given f(varchar(42)):varchar(42) bind f(varchar(1)):varchar(1) no coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                varchar42.getTypeSignature(),
                ImmutableList.of(varchar42.getTypeSignature()),
                Optional.of(varchar1),
                ImmutableList.of(varchar1),
                false,
                null);

        // given f(varchar(42)):varchar(42) bind f(varchar(1)):varchar(1) with coercion
        assertFunctionBind(
                ImmutableList.of(),
                ImmutableList.of(),
                varchar42.getTypeSignature(),
                ImmutableList.of(varchar42.getTypeSignature()),
                Optional.of(varchar1),
                ImmutableList.of(varchar1),
                true,
                ImmutableMap.of());
    }

    private void assertFunctionBind(
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            TypeSignature returnType,
            List<TypeSignature> argumentTypes,
            Optional<Type> actualReturnType,
            List<? extends Type> actualArguments,
            boolean allowCoercion,
            Map<String, Type> expectedBoundParameters)
    {
        Signature function = new Signature(
                "function",
                SCALAR,
                typeVariableConstraints,
                longVariableConstraints,
                returnType,
                argumentTypes,
                false);

        Map<String, Type> actualBoundParameters;
        if (actualReturnType.isPresent()) {
            actualBoundParameters = function.bindTypeVariables(actualReturnType.get(), actualArguments, allowCoercion, new TypeRegistry());
        }
        else {
            actualBoundParameters = function.bindTypeVariables(actualArguments, allowCoercion, new TypeRegistry());
        }

        assertEquals(
                actualBoundParameters,
                expectedBoundParameters);
    }

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
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "T", ImmutableList.of("T"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.<Type>of(BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)"))), true, typeManager));
    }

    @Test
    public void testNonParametric()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(), ImmutableList.of(), "boolean", ImmutableList.of("bigint"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)"))), true, typeManager));
    }

    @Test
    public void testArray()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("get", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "T", ImmutableList.of("array(T)"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)"))), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(BIGINT), true, typeManager));

        signature = new Signature("contains", SCALAR, ImmutableList.of(comparableTypeParameter("T")), ImmutableList.of(), "T", ImmutableList.of("array(T)", "T"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)")), BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)")), VARCHAR), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(HyperLogLog)")), HYPER_LOG_LOG), true, typeManager));

        signature = new Signature("foo", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "T", ImmutableList.of("array(T)", "array(T)"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)")), typeManager.getType(parseTypeSignature("array(bigint)"))), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("array(bigint)")), typeManager.getType(parseTypeSignature("array(varchar)"))), true, typeManager));
    }

    @Test
    public void testMap()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("get", SCALAR, ImmutableList.of(typeVariable("K"), typeVariable("V")), ImmutableList.of(), "V", ImmutableList.of("map(K,V)", "K"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("map(bigint,varchar)")), BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(typeManager.getType(parseTypeSignature("map(bigint,varchar)")), VARCHAR), true, typeManager));
    }

    @Test
    public void testVariadic()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Type mapType = typeManager.getType(parseTypeSignature("map(bigint,bigint)"));
        Type arrayType = typeManager.getType(parseTypeSignature("array(bigint)"));
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(withVariadicBound("T", "map")), ImmutableList.of(), "bigint", ImmutableList.of("T"), true);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(mapType), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(arrayType), true, typeManager));
    }

    @Test
    public void testVarArgs()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "boolean", ImmutableList.of("T"), true);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(BIGINT, BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
    }

    @Test
    public void testCoercion()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "boolean", ImmutableList.of("T", "double"), true);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(DOUBLE, DOUBLE), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(BIGINT, BIGINT), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(VARCHAR, BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));
    }

    @Test
    public void testUnknownCoercion()
            throws Exception
    {
        TypeManager typeManager = new TypeRegistry();
        Signature signature = new Signature("foo", SCALAR, ImmutableList.of(typeVariable("T")), ImmutableList.of(), "boolean", ImmutableList.of("T", "T"), false);
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(UNKNOWN, UNKNOWN), true, typeManager));
        assertNotNull(signature.bindTypeVariables(ImmutableList.of(UNKNOWN, BIGINT), true, typeManager));
        assertNull(signature.bindTypeVariables(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));

        signature = new Signature("foo", SCALAR, ImmutableList.of(comparableTypeParameter("T")), ImmutableList.of(), "boolean", ImmutableList.of("T", "T"), false);
        Map<String, Type> boundParameters = signature.bindTypeVariables(ImmutableList.of(UNKNOWN, BIGINT), true, typeManager);
        assertNotNull(boundParameters);
        assertEquals(boundParameters.get("T"), BIGINT);
        assertNull(signature.bindTypeVariables(ImmutableList.of(BIGINT, VARCHAR), true, typeManager));
    }
}
