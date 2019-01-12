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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.type.FunctionType;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSignatureBinder
{
    private final TypeManager typeRegistry = new TypeRegistry();

    TestSignatureBinder()
    {
        // associate typeRegistry with a function registry
        new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
    }

    @Test
    public void testBindLiteralForDecimal()
    {
        TypeSignature leftType = parseTypeSignature("decimal(p1,s1)", ImmutableSet.of("p1", "s1"));
        TypeSignature rightType = parseTypeSignature("decimal(p2,s2)", ImmutableSet.of("p2", "s2"));

        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo("decimal(2,1)", "decimal(1,0)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "p1", 2L,
                                "s1", 1L,
                                "p2", 1L,
                                "s2", 0L)));
    }

    @Test
    public void testBindPartialDecimal()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("decimal(4,s)", ImmutableSet.of("s")))
                .build();

        assertThat(function)
                .boundTo("decimal(2,1)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("s", 1L)));

        function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("decimal(p,1)", ImmutableSet.of("p")))
                .build();

        assertThat(function)
                .boundTo("decimal(2,0)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 3L)));

        assertThat(function)
                .boundTo("decimal(2,1)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 2L)));

        assertThat(function)
                .boundTo("bigint")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 20L)));
    }

    @Test
    public void testBindLiteralForVarchar()
    {
        TypeSignature leftType = parseTypeSignature("varchar(x)", ImmutableSet.of("x"));
        TypeSignature rightType = parseTypeSignature("varchar(y)", ImmutableSet.of("y"));

        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo("varchar(42)", "varchar(44)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "x", 42L,
                                "y", 44L)));

        assertThat(function)
                .boundTo("unknown", "varchar(44)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "x", 0L,
                                "y", 44L)));
    }

    @Test
    public void testBindLiteralForRepeatedVarcharWithReturn()
    {
        TypeSignature leftType = parseTypeSignature("varchar(x)", ImmutableSet.of("x"));
        TypeSignature rightType = parseTypeSignature("varchar(x)", ImmutableSet.of("x"));

        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo("varchar(44)", "varchar(44)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo("varchar(44)", "varchar(42)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo("varchar(42)", "varchar(44)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
        assertThat(function)
                .boundTo("unknown", "varchar(44)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 44L)));
    }

    @Test
    public void testBindLiteralForRepeatedDecimal()
    {
        TypeSignature leftType = parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s"));
        TypeSignature rightType = parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s"));

        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo("decimal(10,5)", "decimal(10,5)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 5L)));
        assertThat(function)
                .boundTo("decimal(10,8)", "decimal(9,8)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 8L)));
        assertThat(function)
                .boundTo("decimal(10,2)", "decimal(10,8)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 16L, "s", 8L)));
        assertThat(function)
                .boundTo("unknown", "decimal(10,5)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("p", 10L, "s", 5L)));
    }

    @Test
    public void testBindLiteralForRepeatedVarchar()
    {
        Set<String> literalParameters = ImmutableSet.of("x");
        TypeSignature leftType = parseTypeSignature("varchar(x)", literalParameters);
        TypeSignature rightType = parseTypeSignature("varchar(x)", literalParameters);
        TypeSignature returnType = parseTypeSignature("varchar(x)", literalParameters);

        Signature function = functionSignature()
                .returnType(returnType)
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .withCoercion()
                .boundTo(ImmutableList.of("varchar(3)", "varchar(5)"), "varchar(5)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 5L)));

        assertThat(function)
                .withCoercion()
                .boundTo(ImmutableList.of("varchar(3)", "varchar(5)"), "varchar(6)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", 6L)));
    }

    @Test
    public void testBindUnknown()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .build();

        assertThat(function)
                .boundTo("unknown")
                .fails();

        assertThat(function)
                .boundTo("unknown")
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindMixedLiteralAndTypeVariables()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("array(decimal(p,s))", ImmutableSet.of("p", "s")))
                .build();

        assertThat(function)
                .boundTo("array(decimal(2,1))", "array(decimal(3,1))")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T", type("decimal(2,1)")),
                        ImmutableMap.of(
                                "p", 3L,
                                "s", 1L)));
    }

    @Test
    public void testBindDifferentLiteralParameters()
    {
        TypeSignature argType = parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s"));

        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(argType, argType)
                .build();

        assertThat(function)
                .boundTo("decimal(2,1)", "decimal(3,1)")
                .fails();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoVariableReuseAcrossTypes()
    {
        Set<String> literalParameters = ImmutableSet.of("p1", "p2", "s");
        TypeSignature leftType = parseTypeSignature("decimal(p1,s)", literalParameters);
        TypeSignature rightType = parseTypeSignature("decimal(p2,s)", literalParameters);

        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(leftType, rightType)
                .build();

        assertThat(function)
                .boundTo("decimal(2,1)", "decimal(3,1)")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindUnknownToDecimal()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("decimal(p,s)", ImmutableSet.of("p", "s")))
                .build();

        assertThat(function)
                .boundTo("unknown")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "p", 1L,
                                "s", 0L)));
    }

    @Test
    public void testBindUnknownToConcreteArray()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("array(boolean)"))
                .build();

        assertThat(function)
                .boundTo("unknown")
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindTypeVariablesBasedOnTheSecondArgument()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo("unknown", "decimal(2,1)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("decimal(2,1)")),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindParametricTypeParameterToUnknown()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("array(T)"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo("unknown")
                .fails();

        assertThat(function)
                .withCoercion()
                .boundTo("unknown")
                .succeeds();
    }

    @Test
    public void testBindUnknownToTypeParameter()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo("unknown")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("unknown")),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindDoubleToBigint()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE))
                .build();

        assertThat(function)
                .boundTo("double", "bigint")
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBindVarcharTemplateStyle()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("T2"))
                .argumentTypes(parseTypeSignature("T1"))
                .typeVariableConstraints(ImmutableList.of(
                        new TypeVariableConstraint("T1", true, false, "varchar"),
                        new TypeVariableConstraint("T2", true, false, "varchar")))
                .build();

        assertThat(function)
                .boundTo(ImmutableList.of("varchar(42)"), "varchar(1)")
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T1", type("varchar(42)"),
                                "T2", type("varchar(1)")),
                        ImmutableMap.of()));
    }

    @Test
    public void testBindVarchar()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("varchar(42)"))
                .argumentTypes(parseTypeSignature("varchar(42)"))
                .build();

        assertThat(function)
                .boundTo(ImmutableList.of("varchar(1)"), "varchar(1)")
                .fails();

        assertThat(function)
                .boundTo(ImmutableList.of("varchar(1)"), "varchar(1)")
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo(ImmutableList.of("varchar(1)"), "varchar(42)")
                .withCoercion()
                .succeeds();

        assertThat(function)
                .boundTo(ImmutableList.of("varchar(44)"), "varchar(44)")
                .withCoercion()
                .fails();
    }

    @Test
    public void testBindUnparametrizedVarchar()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("varchar(x)", ImmutableSet.of("x")))
                .build();

        assertThat(function)
                .boundTo("varchar")
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of("x", (long) Integer.MAX_VALUE)));
    }

    @Test
    public void testBindToUnparametrizedVarcharIsImpossible()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("varchar"))
                .build();

        assertThat(function)
                .boundTo("varchar(3)")
                .withCoercion()
                .succeeds();

        assertThat(function)
                .boundTo("unknown")
                .withCoercion()
                .succeeds();
    }

    @Test
    public void testBasic()
    {
        Signature function = functionSignature()
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("T"))
                .build();

        assertThat(function)
                .boundTo("bigint")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo("varchar")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("varchar")),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo("varchar", "bigint")
                .fails();

        assertThat(function)
                .boundTo("array(bigint)")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("array(bigint)")),
                        ImmutableMap.of()));
    }

    @Test
    public void testMismatchedArgumentCount()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT))
                .build();

        assertThat(function)
                .boundTo("bigint", "bigint", "bigint")
                .fails();

        assertThat(function)
                .boundTo("bigint")
                .fails();
    }

    @Test
    public void testNonParametric()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature(StandardTypes.BIGINT))
                .build();

        assertThat(function)
                .boundTo("bigint")
                .succeeds();

        assertThat(function)
                .boundTo("varchar")
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo("varchar", "bigint")
                .withCoercion()
                .fails();

        assertThat(function)
                .boundTo("array(bigint)")
                .withCoercion()
                .fails();
    }

    @Test
    public void testArray()
    {
        Signature getFunction = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("array(T)"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(getFunction)
                .boundTo("array(bigint)")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(getFunction)
                .boundTo("bigint")
                .withCoercion()
                .fails();

        assertThat(getFunction)
                .boundTo("row(bigint)")
                .withCoercion()
                .fails();

        Signature containsFunction = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertThat(containsFunction)
                .boundTo("array(bigint)", "bigint")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(containsFunction)
                .boundTo("array(bigint)", "varchar")
                .withCoercion()
                .fails();

        assertThat(containsFunction)
                .boundTo("array(HyperLogLog)", "HyperLogLog")
                .withCoercion()
                .fails();

        Signature castFunction = functionSignature()
                .returnType(parseTypeSignature("array(T2)"))
                .argumentTypes(parseTypeSignature("array(T1)"), parseTypeSignature("array(T2)"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T1"), typeVariable("T2")))
                .build();

        assertThat(castFunction)
                .boundTo("array(unknown)", "array(decimal(2,1))")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "T1", type("unknown"),
                                "T2", type("decimal(2,1)")),
                        ImmutableMap.of()));

        Signature fooFunction = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("array(T)"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(fooFunction)
                .boundTo("array(bigint)", "array(bigint)")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(fooFunction)
                .boundTo("array(bigint)", "array(varchar)")
                .withCoercion()
                .fails();
    }

    @Test
    public void testMap()
    {
        Signature getValueFunction = functionSignature()
                .returnType(parseTypeSignature("V"))
                .argumentTypes(parseTypeSignature("map(K,V)"), parseTypeSignature("K"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("K"), typeVariable("V")))
                .build();

        assertThat(getValueFunction)
                .boundTo("map(bigint,varchar)", "bigint")
                .produces(new BoundVariables(
                        ImmutableMap.of(
                                "K", type("bigint"),
                                "V", type("varchar")),
                        ImmutableMap.of()));

        assertThat(getValueFunction)
                .boundTo("map(bigint,varchar)", "varchar")
                .withCoercion()
                .fails();
    }

    @Test
    public void testRow()
    {
        Signature function = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(parseTypeSignature("row(integer)"))
                .build();

        assertThat(function)
                .boundTo("row(tinyint)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
        assertThat(function)
                .boundTo("row(integer)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of()));
        assertThat(function)
                .boundTo("row(bigint)")
                .withCoercion()
                .fails();

        Signature biFunction = functionSignature()
                .returnType(BOOLEAN.getTypeSignature())
                .argumentTypes(parseTypeSignature("row(T)"), parseTypeSignature("row(T)"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(biFunction)
                .boundTo("row(bigint)", "row(bigint)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));
        assertThat(biFunction)
                .boundTo("row(integer)", "row(bigint)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));
    }

    @Test
    public void testVariadic()
    {
        Signature mapVariadicBoundFunction = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BIGINT))
                .argumentTypes(parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "map")))
                .build();

        assertThat(mapVariadicBoundFunction)
                .boundTo("map(bigint,bigint)")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("map(bigint,bigint)")),
                        ImmutableMap.of()));

        assertThat(mapVariadicBoundFunction)
                .boundTo("array(bigint)")
                .fails();

        assertThat(mapVariadicBoundFunction)
                .boundTo("array(bigint)")
                .withCoercion()
                .fails();

        Signature decimalVariadicBoundFunction = functionSignature()
                .returnType(parseTypeSignature("bigint"))
                .argumentTypes(parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "decimal")))
                .build();

        assertThat(decimalVariadicBoundFunction)
                .boundTo("decimal(2,1)")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("decimal(2,1)")),
                        ImmutableMap.of()));

        assertThat(decimalVariadicBoundFunction)
                .boundTo("bigint")
                .fails();
    }

    @Test
    public void testBindUnknownToVariadic()
    {
        Signature rowFunction = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "row")))
                .build();

        assertThat(rowFunction)
                .boundTo("unknown", "row(a bigint)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("row(a bigint)")),
                        ImmutableMap.of()));

        Signature arrayFunction = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(withVariadicBound("T", "array")))
                .build();

        assertThat(arrayFunction)
                .boundTo("unknown", "array(bigint)")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("array(bigint)")),
                        ImmutableMap.of()));
    }

    @Test
    public void testVarArgs()
    {
        Signature variableArityFunction = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .setVariableArity(true)
                .build();

        assertThat(variableArityFunction)
                .boundTo("bigint")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo("varchar")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("varchar")),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo("bigint", "bigint")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(variableArityFunction)
                .boundTo("bigint", "varchar")
                .withCoercion()
                .fails();
    }

    @Test
    public void testCoercion()
    {
        Signature function = functionSignature()
                .returnType(parseTypeSignature(StandardTypes.BOOLEAN))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature(StandardTypes.DOUBLE))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(function)
                .boundTo("double", "double")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("double")),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo("bigint", "bigint")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo("varchar", "bigint")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("varchar")),
                        ImmutableMap.of()));

        assertThat(function)
                .boundTo("bigint", "varchar")
                .withCoercion()
                .fails();
    }

    @Test
    public void testUnknownCoercion()
    {
        Signature foo = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(typeVariable("T")))
                .build();

        assertThat(foo)
                .boundTo("unknown", "unknown")
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("unknown")),
                        ImmutableMap.of()));

        assertThat(foo)
                .boundTo("unknown", "bigint")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(foo)
                .boundTo("varchar", "bigint")
                .withCoercion()
                .fails();

        Signature bar = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("T"))
                .typeVariableConstraints(ImmutableList.of(comparableTypeParameter("T")))
                .build();

        assertThat(bar)
                .boundTo("unknown", "bigint")
                .withCoercion()
                .produces(new BoundVariables(
                        ImmutableMap.of("T", type("bigint")),
                        ImmutableMap.of()));

        assertThat(bar)
                .boundTo("varchar", "bigint")
                .withCoercion()
                .fails();

        assertThat(bar)
                .boundTo("HyperLogLog", "HyperLogLog")
                .withCoercion()
                .fails();
    }

    @Test
    public void testFunction()
    {
        Signature simple = functionSignature()
                .returnType(parseTypeSignature("boolean"))
                .argumentTypes(parseTypeSignature("function(integer,integer)"))
                .build();

        assertThat(simple)
                .boundTo("integer")
                .fails();
        assertThat(simple)
                .boundTo("function(integer,integer)")
                .succeeds();
        // TODO: Support coercion of return type of lambda
        assertThat(simple)
                .boundTo("function(integer,smallint)")
                .withCoercion()
                .fails();
        assertThat(simple)
                .boundTo("function(integer,bigint)")
                .withCoercion()
                .fails();

        Signature applyTwice = functionSignature()
                .returnType(parseTypeSignature("V"))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("function(T,U)"), parseTypeSignature("function(U,V)"))
                .typeVariableConstraints(typeVariable("T"), typeVariable("U"), typeVariable("V"))
                .build();
        assertThat(applyTwice)
                .boundTo("integer", "integer", "integer")
                .fails();
        assertThat(applyTwice)
                .boundTo("integer", "function(integer,varchar)", "function(varchar,double)")
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("U", VARCHAR)
                        .setTypeVariable("V", DOUBLE)
                        .build());
        assertThat(applyTwice)
                .boundTo(
                        "integer",
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(integer,varchar)")),
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(varchar,double)")))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("U", VARCHAR)
                        .setTypeVariable("V", DOUBLE)
                        .build());
        assertThat(applyTwice)
                .boundTo(
                        // pass function argument to non-function position of a function
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(integer,varchar)")),
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(integer,varchar)")),
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(varchar,double)")))
                .fails();
        assertThat(applyTwice)
                .boundTo(
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(integer,varchar)")),
                        // pass non-function argument to function position of a function
                        "integer",
                        new TypeSignatureProvider(functionArgumentTypes -> TypeSignature.parseTypeSignature("function(varchar,double)")))
                .fails();

        Signature flatMap = functionSignature()
                .returnType(parseTypeSignature("array(T)"))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("function(T, array(T))"))
                .typeVariableConstraints(typeVariable("T"))
                .build();
        assertThat(flatMap)
                .boundTo("array(integer)", "function(integer, array(integer))")
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .build());

        Signature varargApply = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("function(T, T)"))
                .typeVariableConstraints(typeVariable("T"))
                .setVariableArity(true)
                .build();
        assertThat(varargApply)
                .boundTo("integer", "function(integer, integer)", "function(integer, integer)", "function(integer, integer)")
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .build());
        assertThat(varargApply)
                .boundTo("integer", "function(integer, integer)", "function(integer, double)", "function(double, double)")
                .fails();

        Signature loop = functionSignature()
                .returnType(parseTypeSignature("T"))
                .argumentTypes(parseTypeSignature("T"), parseTypeSignature("function(T, T)"))
                .typeVariableConstraints(typeVariable("T"))
                .build();
        assertThat(loop)
                .boundTo("integer", new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, BIGINT).getTypeSignature()))
                .fails();
        assertThat(loop)
                .boundTo("integer", new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, BIGINT).getTypeSignature()))
                .withCoercion()
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", BIGINT)
                        .build());
        // TODO: Support coercion of return type of lambda
        assertThat(loop)
                .withCoercion()
                .boundTo("integer", new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, SMALLINT).getTypeSignature()))
                .fails();

        // TODO: Support coercion of return type of lambda
        // Without coercion support for return type of lambda, the return type of lambda must be `varchar(x)` to avoid need for coercions.
        Signature varcharApply = functionSignature()
                .returnType(parseTypeSignature("varchar"))
                .argumentTypes(parseTypeSignature("varchar"), parseTypeSignature("function(varchar, varchar(x))", ImmutableSet.of("x")))
                .build();
        assertThat(varcharApply)
                .withCoercion()
                .boundTo("varchar(10)", new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, createVarcharType(1)).getTypeSignature()))
                .succeeds();

        Signature sortByKey = functionSignature()
                .returnType(parseTypeSignature("array(T)"))
                .argumentTypes(parseTypeSignature("array(T)"), parseTypeSignature("function(T,E)"))
                .typeVariableConstraints(typeVariable("T"), orderableTypeParameter("E"))
                .build();
        assertThat(sortByKey)
                .boundTo("array(integer)", new TypeSignatureProvider(paramTypes -> new FunctionType(paramTypes, VARCHAR).getTypeSignature()))
                .produces(BoundVariables.builder()
                        .setTypeVariable("T", INTEGER)
                        .setTypeVariable("E", VARCHAR)
                        .build());
    }

    @Test
    public void testBindParameters()
    {
        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T1", DOUBLE)
                .setTypeVariable("T2", BIGINT)
                .setTypeVariable("T3", DecimalType.createDecimalType(5, 3))
                .setLongVariable("p", 1L)
                .setLongVariable("s", 2L)
                .build();

        assertThat("bigint", boundVariables, "bigint");
        assertThat("T1", boundVariables, "double");
        assertThat("T2", boundVariables, "bigint");
        assertThat("array(T1)", boundVariables, "array(double)");
        assertThat("array(T3)", boundVariables, "array(decimal(5,3))");
        assertThat("array<T1>", boundVariables, "array(double)");
        assertThat("map(T1,T2)", boundVariables, "map(double,bigint)");
        assertThat("map<T1,T2>", boundVariables, "map(double,bigint)");
        assertThat("bla(T1,42,T2)", boundVariables, "bla(double,42,bigint)");
        assertThat("varchar(p)", boundVariables, "varchar(1)");
        assertThat("char(p)", boundVariables, "char(1)");
        assertThat("decimal(p,s)", boundVariables, "decimal(1,2)");
        assertThat("array(decimal(p,s))", boundVariables, "array(decimal(1,2))");

        assertBindVariablesFails("T1(bigint)", boundVariables, "Unbounded parameters can not have parameters");
    }

    private static void assertBindVariablesFails(String typeSignature, BoundVariables boundVariables, String reason)
    {
        try {
            SignatureBinder.applyBoundVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables);
            fail(reason);
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    private static void assertThat(String typeSignature, BoundVariables boundVariables, String expectedTypeSignature)
    {
        assertEquals(
                SignatureBinder.applyBoundVariables(parseTypeSignature(typeSignature, ImmutableSet.of("p", "s")), boundVariables).toString(),
                expectedTypeSignature);
    }

    private static SignatureBuilder functionSignature()
    {
        return new SignatureBuilder()
                .name("function")
                .kind(SCALAR);
    }

    private Type type(String signature)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(signature);
        return requireNonNull(typeRegistry.getType(typeSignature));
    }

    private List<Type> types(String... signatures)
    {
        return ImmutableList.copyOf(signatures)
                .stream()
                .map(this::type)
                .collect(toList());
    }

    private BindSignatureAssertion assertThat(Signature function)
    {
        return new BindSignatureAssertion(function);
    }

    private class BindSignatureAssertion
    {
        private final Signature function;
        private List<TypeSignatureProvider> argumentTypes;
        private Type returnType;
        private boolean allowCoercion;

        private BindSignatureAssertion(Signature function)
        {
            this.function = function;
        }

        public BindSignatureAssertion withCoercion()
        {
            allowCoercion = true;
            return this;
        }

        public BindSignatureAssertion boundTo(Object... arguments)
        {
            ImmutableList.Builder<TypeSignatureProvider> builder = ImmutableList.builder();
            for (Object argument : arguments) {
                if (argument instanceof String) {
                    builder.add(new TypeSignatureProvider(TypeSignature.parseTypeSignature((String) argument)));
                    continue;
                }
                if (argument instanceof TypeSignatureProvider) {
                    builder.add((TypeSignatureProvider) argument);
                    continue;
                }
                throw new IllegalArgumentException(format("argument is of type %s. It should be String or TypeSignatureProvider", argument.getClass()));
            }
            this.argumentTypes = builder.build();
            return this;
        }

        public BindSignatureAssertion boundTo(List<String> arguments, String returnType)
        {
            this.argumentTypes = fromTypes(types(arguments.toArray(new String[arguments.size()])));
            this.returnType = type(returnType);
            return this;
        }

        public BindSignatureAssertion succeeds()
        {
            assertTrue(bindVariables().isPresent());
            return this;
        }

        public BindSignatureAssertion fails()
        {
            assertFalse(bindVariables().isPresent());
            return this;
        }

        public BindSignatureAssertion produces(BoundVariables expected)
        {
            Optional<BoundVariables> actual = bindVariables();
            assertTrue(actual.isPresent());
            assertEquals(actual.get(), expected);
            return this;
        }

        private Optional<BoundVariables> bindVariables()
        {
            assertNotNull(argumentTypes);
            SignatureBinder signatureBinder = new SignatureBinder(typeRegistry, function, allowCoercion);
            if (returnType == null) {
                return signatureBinder.bindVariables(argumentTypes);
            }
            else {
                return signatureBinder.bindVariables(argumentTypes, returnType);
            }
        }
    }
}
