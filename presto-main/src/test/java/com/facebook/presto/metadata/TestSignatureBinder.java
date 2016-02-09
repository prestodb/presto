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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.longVariableExpression;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSignatureBinder
{
    private final TypeRegistry typeRegistry = new TypeRegistry();

    @Test
    public void testBindLiteralForDecimal()
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("decimal(p1,s1)", "decimal(p2,s2)")
                .literalParameters("p1", "s1", "p2", "s2")
                .build();

        assertBindSignature(function)
                .toArguments("decimal(2,1)", "decimal(1,0)")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p1", 2L,
                        "s1", 1L,
                        "p2", 1L,
                        "s2", 0L
                ));
    }

    @Test
    public void testResolveCalculatedTypes()
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("varchar(x)", "varchar(y)")
                .literalParameters("x", "y")
                .build();

        assertBindSignature(function)
                .toArguments("varchar(42)", "varchar(44)")
                .hasLongVariablesBound(ImmutableMap.of(
                        "x", 42L,
                        "y", 44L
                ));

        assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown", "varchar(44)")
                .hasLongVariablesBound(ImmutableMap.of(
                        "x", 0L,
                        "y", 44L
                ));
    }

    @Test
    public void testBindUnknown()
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("varchar(x)")
                .literalParameters("x")
                .build();

        assertBindSignature(function)
                .toArguments("unknown")
                .isFailed();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown")
                .isSuccessfull();
    }

    @Test
    public void testBindBigintToDecimal()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("decimal(p,s)")
                .literalParameters("p", "s")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("bigint")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p", 19L,
                        "s", 0L
                ));
    }

    @Test
    public void testBindDecimalBigintToDecimalDecimal()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("decimal(p1,s1)", "decimal(p2,s2)")
                .literalParameters("p1", "s1", "p2", "s2")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("decimal(5,2)", "bigint")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p1", 5L,
                        "s1", 2L,
                        "p2", 19L,
                        "s2", 0L
                ));
    }

    @Test
    public void testBindMixedLiteralAndTypeVariables()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .argumentTypes("array(T)", "array(decimal(p,s))")
                .literalParameters("p", "s")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("array(decimal(2,1))", "array(decimal(3,1))")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p", 3L,
                        "s", 1L
                ))
                .hasTypeVariablesBound(ImmutableMap.of(
                        "T", "decimal(2,1)"
                ));
    }

    @Test
    public void testBindDifferentLiteralParameters()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("decimal(p,s)", "decimal(p,s)")
                .literalParameters("p", "s")
                .build();

        assertBindSignature(function)
                .toArguments("decimal(2,1)", "decimal(3,1)")
                .isFailed();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("decimal(2,1)", "decimal(3,1)")
                .isFailed();
    }

    @Test
    public void testBindCalculatedLiteralParameter()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("decimal(p3,s)")
                .argumentTypes("decimal(p1,s)", "decimal(p2,s)")
                .literalParameters("p1", "p2", "p3", "s")
                .longVariableConstraints(longVariableExpression("p3", "p1 + p2"))
                .build();

        assertBindSignature(function)
                .toArguments("decimal(2,1)", "decimal(3,1)")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p1", 2L, "p2", 3L, "p3", 5L, "s", 1L
                ));
    }

    @Test
    public void testBindUnknownToDecimal()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("decimal(p,s)")
                .literalParameters("p", "s")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown")
                .hasLongVariablesBound(ImmutableMap.of(
                        "p", 1L,
                        "s", 0L
                ));
    }

    @Test
    public void testBindDecimalToDouble()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("double")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("decimal(5,2)")
                .isSuccessfull();
    }

    @Test
    public void testBindUnknownToConcreteArray()
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("array(boolean)")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown")
                .isSuccessfull();
    }

    @Test
    public void testBindUnknownToArray()
    {
        Signature function = functionSignature()
                .returnType("T")
                .argumentTypes("array(T)")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertBindSignature(function)
                .toArguments("unknown")
                .isFailed();

        // TODO: fix this
        /*assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown")
                .isSuccessfull();*/
    }

    @Test
    public void testBindDoubleToBigint()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("double", "double")
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("double", "bigint")
                .isSuccessfull();
    }

    @Test
    public void testBindVarcharTemplateStyle()
    {
        Signature function = functionSignature()
                .returnType("T2")
                .argumentTypes("T1")
                .typeVariableConstraints(ImmutableList.of(
                        new TypeVariableConstraint("T1", true, false, "varchar"),
                        new TypeVariableConstraint("T2", true, false, "varchar")
                ))
                .build();

        assertBindSignature(function)
                .toArgumentsAndReturnType(ImmutableList.of("varchar(42)"), "varchar(1)")
                .hasTypeVariablesBound(ImmutableMap.of(
                        "T1", "varchar(42)",
                        "T2", "varchar(1)"
                ));
    }

    @Test
    public void testBindVarchar()
    {
        Signature function = functionSignature()
                .returnType("varchar(42)")
                .argumentTypes("varchar(42)")
                .build();

        assertBindSignature(function)
                .toArgumentsAndReturnType(ImmutableList.of("varchar(1)"), "varchar(1)")
                .isFailed();

        assertBindSignature(function)
                .withCoercion()
                .toArgumentsAndReturnType(ImmutableList.of("varchar(1)"), "varchar(1)")
                .isSuccessfull();

        assertBindSignature(function)
                .withCoercion()
                .toArgumentsAndReturnType(ImmutableList.of("varchar(44)"), "varchar(44)")
                .isFailed();
    }

    @Test
    public void testBindUnparametrizedVarchar()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("varchar(x)")
                .literalParameters("x")
                .build();

        assertBindSignature(function)
                .toArguments("varchar")
                .hasLongVariablesBound(ImmutableMap.of("x", (long) Integer.MAX_VALUE));
    }

    @Test
    public void testBindToUnparametrizedVarchar()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("varchar")
                .build();

        assertBindSignature(function)
                .toArguments("varchar(3)")
                .isSuccessfull();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("unknown")
                .isSuccessfull();
    }

    @Test
    public void testBasic()
            throws Exception
    {
        Signature function = functionSignature()
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .returnType("T")
                .argumentTypes("T")
                .build();

        assertBindSignature(function)
                .toArguments("bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(function)
                .toArguments("varchar")
                .hasTypeVariablesBound(ImmutableMap.of("T", "varchar"));

        assertBindSignature(function)
                .toArguments("varchar", "bigint")
                .isFailed();

        assertBindSignature(function)
                .toArguments("array(bigint)")
                .hasTypeVariablesBound(ImmutableMap.of("T", "array(bigint)"));
    }

    @Test
    public void testNonParametric()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("bigint")
                .build();

        assertBindSignature(function)
                .toArguments("bigint")
                .isSuccessfull();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("varchar")
                .isFailed();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("varchar", "bigint")
                .isFailed();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("array(bigint)")
                .isFailed();
    }

    @Test
    public void testArray()
            throws Exception
    {
        Signature getFunction = functionSignature()
                .returnType("T")
                .argumentTypes("array(T)")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertBindSignature(getFunction)
                .toArguments("array(bigint)")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(getFunction)
                .withCoercion()
                .toArguments("bigint")
                .isFailed();

        Signature containsFunction = functionSignature()
                .returnType("T")
                .argumentTypes("array(T)", "T")
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertBindSignature(containsFunction)
                .toArguments("array(bigint)", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(containsFunction)
                .withCoercion()
                .toArguments("array(bigint)", "varchar")
                .isFailed();

        assertBindSignature(containsFunction)
                .withCoercion()
                .toArguments("array(HyperLogLog)", "HyperLogLog")
                .isFailed();

        Signature fooFunction = functionSignature()
                .returnType("T")
                .argumentTypes("array(T)", "array(T)")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertBindSignature(fooFunction)
                .toArguments("array(bigint)", "array(bigint)")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(fooFunction)
                .withCoercion()
                .toArguments("array(bigint)", "array(varchar)")
                .isFailed();
    }

    @Test
    public void testMap()
            throws Exception
    {
        Signature getValueFunction = functionSignature()
                .returnType("V")
                .argumentTypes("map(K,V)", "K")
                .typeVariableConstraints(ImmutableList.of(typeVariable("K"), typeVariable("V")))
                .build();

        assertBindSignature(getValueFunction)
                .toArguments("map(bigint,varchar)", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of(
                        "K", "bigint",
                        "V", "varchar"
                ));

        assertBindSignature(getValueFunction)
                .withCoercion()
                .toArguments("map(bigint,varchar)", "varchar")
                .isFailed();
    }

    @Test
    public void testVariadic()
            throws Exception
    {
        Signature mapVariadicBoundFunction = functionSignature()
                .returnType("bigint")
                .argumentTypes("T")
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "map")))
                .build();

        assertBindSignature(mapVariadicBoundFunction)
                .toArguments("map(bigint,bigint)")
                .hasTypeVariablesBound(ImmutableMap.of("T", "map(bigint,bigint)"));

        assertBindSignature(mapVariadicBoundFunction)
                .toArguments("array(bigint)")
                .isFailed();

        assertBindSignature(mapVariadicBoundFunction)
                .withCoercion()
                .toArguments("array(bigint)")
                .isFailed();

        Signature decimalVariadicBoundFunction = functionSignature()
                .returnType("bigint")
                .argumentTypes("T")
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "decimal")))
                .build();

        assertBindSignature(decimalVariadicBoundFunction)
                .toArguments("decimal(2,1)")
                .hasTypeVariablesBound(ImmutableMap.of("T", "decimal(2,1)"));

        assertBindSignature(decimalVariadicBoundFunction)
                .toArguments("bigint")
                .isFailed();

        assertBindSignature(decimalVariadicBoundFunction)
                .withCoercion()
                .toArguments("bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "decimal(19,0)"));
    }

    @Test
    public void testVarArgs()
            throws Exception
    {
        Signature variableArityFunction = functionSignature()
                .returnType("boolean")
                .argumentTypes("T")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .setVariableArity(true)
                .build();

        assertBindSignature(variableArityFunction)
                .toArguments("bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(variableArityFunction)
                .toArguments("varchar")
                .hasTypeVariablesBound(ImmutableMap.of("T", "varchar"));

        assertBindSignature(variableArityFunction)
                .toArguments("bigint", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(variableArityFunction)
                .withCoercion()
                .toArguments("bigint", "varchar")
                .isFailed();
    }

    @Test
    public void testCoercion()
            throws Exception
    {
        Signature function = functionSignature()
                .returnType("boolean")
                .argumentTypes("T", "double")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertBindSignature(function)
                .withCoercion()
                .toArguments("double", "double")
                .hasTypeVariablesBound(ImmutableMap.of("T", "double"));

        assertBindSignature(function)
                .withCoercion()
                .toArguments("bigint", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(function)
                .withCoercion()
                .toArguments("varchar", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "varchar"));

        assertBindSignature(function)
                .withCoercion()
                .toArguments("bigint", "varchar")
                .isFailed();
    }

    @Test
    public void testUnknownCoercion()
            throws Exception
    {
        Signature foo = functionSignature()
                .returnType("boolean")
                .argumentTypes("T", "T")
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertBindSignature(foo)
                .toArguments("unknown", "unknown")
                .hasTypeVariablesBound(ImmutableMap.of("T", "unknown"));

        assertBindSignature(foo)
                .withCoercion()
                .toArguments("unknown", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(foo)
                .withCoercion()
                .toArguments("varchar", "bigint")
                .isFailed();

        Signature bar = functionSignature()
                .returnType("boolean")
                .argumentTypes("T", "T")
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertBindSignature(bar)
                .withCoercion()
                .toArguments("unknown", "bigint")
                .hasTypeVariablesBound(ImmutableMap.of("T", "bigint"));

        assertBindSignature(bar)
                .withCoercion()
                .toArguments("varchar", "bigint")
                .isFailed();

        assertBindSignature(bar)
                .withCoercion()
                .toArguments("HyperLogLog", "HyperLogLog")
                .isFailed();
    }

    @Test
    public void testBindParameters()
            throws Exception
    {
        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T1", DoubleType.DOUBLE)
                .setTypeVariable("T2", BigintType.BIGINT)
                .setTypeVariable("T3", DecimalType.createDecimalType(5, 3))
                .setLongVariable("p", 1L)
                .setLongVariable("s", 2L)
                .build();

        assertBindSignature("bigint", boundVariables, "bigint");
        assertBindSignature("T1", boundVariables, "double");
        assertBindSignature("T2", boundVariables, "bigint");
        assertBindSignature("array(T1)", boundVariables, "array(double)");
        assertBindSignature("array(T3)", boundVariables, "array(decimal(5,3))");
        assertBindSignature("array<T1>", boundVariables, "array(double)");
        assertBindSignature("map(T1,T2)", boundVariables, "map(double,bigint)");
        assertBindSignature("map<T1,T2>", boundVariables, "map(double,bigint)");
        assertBindSignature("row<T1,T2>('a','b')", boundVariables, "row<double,bigint>('a','b')");
        assertBindSignature("bla(T1,42,T2)", boundVariables, "bla(double,42,bigint)");
        assertBindSignature("varchar(p)", boundVariables, "varchar(1)");
        assertBindSignature("decimal(p,s)", boundVariables, "decimal(1,2)");
        assertBindSignature("array(decimal(p,s))", boundVariables, "array(decimal(1,2))");

        assertBindVariablesFails("T1(bigint)", boundVariables, "Unbounded parameters can not have parameters");
    }

    private void assertBindVariablesFails(String typeSignature, BoundVariables boundVariables, String reason)
    {
        try {
            bindVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables);
            fail(reason);
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private void assertBindSignature(String typeSignature, BoundVariables boundVariables, String expectedTypeSignature)
    {
        assertEquals(
                bindVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables).toString(),
                expectedTypeSignature
        );
    }

    private SignatureBuilder functionSignature()
    {
        return new SignatureBuilder()
                .name("function")
                .kind(SCALAR);
    }

    private Type type(String signature)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(signature);
        return typeRegistry.getType(typeSignature);
    }

    private List<Type> types(String... signatures)
    {
        return ImmutableList.copyOf(signatures)
                .stream()
                .map(this::type)
                .collect(toList());
    }

    private BindSignatureAssertion assertBindSignature(Signature function)
    {
        return new BindSignatureAssertion(function);
    }

    private class BindSignatureAssertion
    {
        private final Signature function;

        private boolean allowCoercion = false;
        private Optional<BoundVariables> boundVariables = Optional.empty();

        private BindSignatureAssertion(Signature function)
        {
            this.function = function;
        }

        public BindSignatureAssertion withCoercion()
        {
            allowCoercion = true;
            return this;
        }

        public BindSignatureAssertion toArguments(String... arguments)
        {
            SignatureBinder signatureBinder = new SignatureBinder(typeRegistry, function, allowCoercion);
            boundVariables = signatureBinder.matchAndBindSignatureVariables(types(arguments));
            return this;
        }

        public BindSignatureAssertion toArgumentsAndReturnType(List<String> arguments, String returnTypeSignature)
        {
            SignatureBinder signatureBinder = new SignatureBinder(typeRegistry, function, allowCoercion);
            List<Type> argumentTypes = types(arguments.toArray(new String[arguments.size()]));
            Type returnType = type(returnTypeSignature);
            boundVariables = signatureBinder.matchAndBindSignatureVariables(argumentTypes, returnType);
            return this;
        }

        public BindSignatureAssertion isSuccessfull()
        {
            assertTrue(boundVariables.isPresent());
            return this;
        }

        public BindSignatureAssertion isFailed()
        {
            assertFalse(boundVariables.isPresent());
            return this;
        }

        public BindSignatureAssertion hasLongVariablesBound(Map<String, Long> longVariables)
        {
            isSuccessfull();
            assertEquals(boundVariables.get().getLongVariables(), longVariables);
            return this;
        }

        public BindSignatureAssertion hasTypeVariablesBound(Map<String, String> typeVariables)
        {
            isSuccessfull();
            Map<String, Type> typesMap = typeVariables.entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, v -> type(v.getValue())));
            assertEquals(boundVariables.get().getTypeVariables(), typesMap);
            return this;
        }
    }
}
