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
package com.facebook.presto.operator;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.operator.annotations.LiteralImplementationDependency;
import com.facebook.presto.operator.annotations.TypeImplementationDependency;
import com.facebook.presto.operator.scalar.ParametricScalar;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.LiteralParameter;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAnnotationEngineForScalars
        extends TestAnnotationEngine
{
    @ScalarFunction("single_implementation_parametric_scalar")
    @Description("Simple scalar with single implementation based on class")
    public static class SingleImplementationScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testSingleImplementationScalarParse()
    {
        Signature expectedSignature = new Signature(
                "single_implementation_parametric_scalar",
                FunctionKind.SCALAR,
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SingleImplementationScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with single implementation based on class");

        assertImplementationCount(scalar, 1, 0, 0);

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());

        assertEquals(specialized.getArgumentProperty(0).getNullConvention(), RETURN_NULL_ON_NULL);
    }

    @ScalarFunction(value = "hidden_scalar_function", hidden = true)
    @Description("Simple scalar with hidden property set")
    public static class HiddenScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testHiddenScalarParse()
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(HiddenScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertTrue(scalar.isDeterministic());
        assertTrue(scalar.isHidden());
    }

    @ScalarFunction(value = "non_deterministic_scalar_function", deterministic = false)
    @Description("Simple scalar with deterministic property reset")
    public static class NonDeterministicScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testNonDeterministicScalarParse()
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(NonDeterministicScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertFalse(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
    }

    @ScalarFunction("scalar_with_nullable")
    @Description("Simple scalar with nullable primitive")
    public static class WithNullablePrimitiveArgScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(
                @SqlType(StandardTypes.DOUBLE) double v,
                @SqlType(StandardTypes.DOUBLE) double v2,
                @IsNull boolean v2isNull)
        {
            return v;
        }
    }

    @Test
    public void testWithNullablePrimitiveArgScalarParse()
    {
        Signature expectedSignature = new Signature(
                "scalar_with_nullable",
                FunctionKind.SCALAR,
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullablePrimitiveArgScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with nullable primitive");

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 2, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());

        assertEquals(specialized.getArgumentProperty(0), valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        assertEquals(specialized.getArgumentProperty(1), valueTypeArgumentProperty(USE_NULL_FLAG));
    }

    @ScalarFunction("scalar_with_nullable_complex")
    @Description("Simple scalar with nullable complex type")
    public static class WithNullableComplexArgScalarFunction
    {
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(
                @SqlType(StandardTypes.DOUBLE) double v,
                @SqlNullable @SqlType(StandardTypes.DOUBLE) Double v2)
        {
            return v;
        }
    }

    @Test
    public void testWithNullableComplexArgScalarParse()
    {
        Signature expectedSignature = new Signature(
                "scalar_with_nullable_complex",
                FunctionKind.SCALAR,
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullableComplexArgScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with nullable complex type");

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 2, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());

        assertEquals(specialized.getArgumentProperty(0), valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        assertEquals(specialized.getArgumentProperty(1), valueTypeArgumentProperty(USE_BOXED_TYPE));
    }

    public static class StaticMethodScalarFunction
    {
        @ScalarFunction("static_method_scalar")
        @Description("Simple scalar with single implementation based on method")
        @SqlType(StandardTypes.DOUBLE)
        public static double fun(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }
    }

    @Test
    public void testStaticMethodScalarParse()
    {
        Signature expectedSignature = new Signature(
                "static_method_scalar",
                FunctionKind.SCALAR,
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(StaticMethodScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with single implementation based on method");
    }

    public static class MultiScalarFunction
    {
        @ScalarFunction("static_method_scalar_1")
        @Description("Simple scalar with single implementation based on method 1")
        @SqlType(StandardTypes.DOUBLE)
        public static double fun1(@SqlType(StandardTypes.DOUBLE) double v)
        {
            return v;
        }

        @ScalarFunction(value = "static_method_scalar_2", hidden = true, deterministic = false)
        @Description("Simple scalar with single implementation based on method 2")
        @SqlType(StandardTypes.BIGINT)
        public static long fun2(@SqlType(StandardTypes.BIGINT) long v)
        {
            return v;
        }
    }

    @Test
    public void testMultiScalarParse()
    {
        Signature expectedSignature1 = new Signature(
                "static_method_scalar_1",
                FunctionKind.SCALAR,
                DOUBLE.getTypeSignature(),
                ImmutableList.of(DOUBLE.getTypeSignature()));

        Signature expectedSignature2 = new Signature(
                "static_method_scalar_2",
                FunctionKind.SCALAR,
                BIGINT.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(MultiScalarFunction.class);
        assertEquals(functions.size(), 2);
        ParametricScalar scalar1 = (ParametricScalar) functions.stream().filter(signature -> signature.getSignature().equals(expectedSignature1)).collect(toImmutableList()).get(0);
        ParametricScalar scalar2 = (ParametricScalar) functions.stream().filter(signature -> signature.getSignature().equals(expectedSignature2)).collect(toImmutableList()).get(0);

        assertImplementationCount(scalar1, 1, 0, 0);
        assertImplementationCount(scalar2, 1, 0, 0);

        assertEquals(scalar1.getSignature(), expectedSignature1);
        assertTrue(scalar1.isDeterministic());
        assertFalse(scalar1.isHidden());
        assertEquals(scalar1.getDescription(), "Simple scalar with single implementation based on method 1");

        assertEquals(scalar2.getSignature(), expectedSignature2);
        assertFalse(scalar2.isDeterministic());
        assertTrue(scalar2.isHidden());
        assertEquals(scalar2.getDescription(), "Simple scalar with single implementation based on method 2");
    }

    @ScalarFunction("parametric_scalar")
    @Description("Parametric scalar description")
    public static class ParametricScalarFunction
    {
        @SqlType("T")
        @TypeParameter("T")
        public static double fun(@SqlType("T") double v)
        {
            return v;
        }

        @SqlType("T")
        @TypeParameter("T")
        public static long fun(@SqlType("T") long v)
        {
            return v;
        }
    }

    @Test
    public void testParametricScalarParse()
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ParametricScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 2, 0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar description");
    }

    @ScalarFunction("with_exact_scalar")
    @Description("Parametric scalar with exact and generic implementations")
    public static class ComplexParametricScalarFunction
    {
        @SqlType(StandardTypes.BOOLEAN)
        @LiteralParameters("x")
        public static boolean fun1(@SqlType("array(varchar(x))") Block array)
        {
            return true;
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean fun2(@SqlType("array(varchar(17))") Block array)
        {
            return true;
        }
    }

    @Test
    public void testComplexParametricScalarParse()
    {
        Signature expectedSignature = new Signature(
                "with_exact_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(varchar(x))", ImmutableSet.of("x"))),
                false);

        Signature exactSignature = new Signature(
                "with_exact_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(varchar(17))")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ComplexParametricScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar.getImplementations(), 1, 0, 1);
        assertEquals(getOnlyElement(scalar.getImplementations().getExactImplementations().keySet()), exactSignature);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with exact and generic implementations");
    }

    @ScalarFunction("parametric_scalar_inject")
    @Description("Parametric scalar with literal injected")
    public static class SimpleInjectionScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        @LiteralParameters("x")
        public static long fun(
                @LiteralParameter("x") Long literalParam,
                @SqlType("varchar(x)") Slice val)
        {
            return literalParam;
        }
    }

    @Test
    public void testSimpleInjectionScalarParse()
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar_inject",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("varchar(x)", ImmutableSet.of("x"))),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SimpleInjectionScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 0, 1);
        List<ImplementationDependency> dependencies = scalar.getImplementations().getGenericImplementations().get(0).getDependencies();
        assertEquals(dependencies.size(), 1);
        assertTrue(dependencies.get(0) instanceof LiteralImplementationDependency);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with literal injected");
    }

    @ScalarFunction("parametric_scalar_inject_constructor")
    @Description("Parametric scalar with type injected though constructor")
    public static class ConstructorInjectionScalarFunction
    {
        private final Type type;

        @TypeParameter("T")
        public ConstructorInjectionScalarFunction(@TypeParameter("T") Type type)
        {
            this.type = type;
        }

        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("T")
        public long fun(@SqlType("array(T)") Block val)
        {
            return 17L;
        }

        @SqlType(StandardTypes.BIGINT)
        public long funBigint(@SqlType("array(bigint)") Block val)
        {
            return 17L;
        }

        @SqlType(StandardTypes.BIGINT)
        public long funDouble(@SqlType("array(double)") Block val)
        {
            return 17L;
        }
    }

    @Test
    public void testConstructorInjectionScalarParse()
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar_inject_constructor",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(T)")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ConstructorInjectionScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 2, 0, 1);
        List<ImplementationDependency> dependencies = scalar.getImplementations().getGenericImplementations().get(0).getDependencies();
        assertEquals(dependencies.size(), 0);
        List<ImplementationDependency> constructorDependencies = scalar.getImplementations().getGenericImplementations().get(0).getConstructorDependencies();
        assertEquals(constructorDependencies.size(), 1);
        assertTrue(constructorDependencies.get(0) instanceof TypeImplementationDependency);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with type injected though constructor");
    }

    @ScalarFunction("fixed_type_parameter_scalar_function")
    @Description("Parametric scalar that uses TypeParameter with fixed type")
    public static final class FixedTypeParameterScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        public static long fun(
                @TypeParameter("ROW(ARRAY(BIGINT),ROW(ROW(CHAR)),BIGINT,MAP(BIGINT,CHAR))") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testFixedTypeParameterParse()
    {
        Signature expectedSignature = new Signature(
                "fixed_type_parameter_scalar_function",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature()),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(FixedTypeParameterScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 1, 0, 0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar that uses TypeParameter with fixed type");
    }

    @ScalarFunction("partially_fixed_type_parameter_scalar_function")
    @Description("Parametric scalar that uses TypeParameter with partially fixed type")
    public static final class PartiallyFixedTypeParameterScalarFunction
    {
        @SqlType(StandardTypes.BIGINT)
        @TypeParameter("T1")
        @TypeParameter("T2")
        public static long fun(
                @TypeParameter("ROW(ARRAY(T1),ROW(ROW(T2)),CHAR)") Type type,
                @SqlType(StandardTypes.BIGINT) long value)
        {
            return value;
        }
    }

    @Test
    public void testPartiallyFixedTypeParameterParse()
    {
        Signature expectedSignature = new Signature(
                "partially_fixed_type_parameter_scalar_function",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T1"), typeVariable("T2")),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(BIGINT.getTypeSignature()),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(PartiallyFixedTypeParameterScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertImplementationCount(scalar, 0, 0, 1);
        List<ImplementationDependency> dependencies = scalar.getImplementations().getGenericImplementations().get(0).getDependencies();
        assertEquals(dependencies.size(), 1);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar that uses TypeParameter with partially fixed type");
    }
}
