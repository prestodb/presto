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
package io.prestosql.sql.gen;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.type.TypeManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.gen.TestVarArgsToArrayAdapterGenerator.TestVarArgsSum.VAR_ARGS_SUM;
import static io.prestosql.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;

public class TestVarArgsToArrayAdapterGenerator
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(VAR_ARGS_SUM);
    }

    @Test
    public void testArrayElements()
    {
        assertFunction("var_args_sum()", INTEGER, 0);
        assertFunction("var_args_sum(1)", INTEGER, 1);
        assertFunction("var_args_sum(1, 2)", INTEGER, 3);
        assertFunction("var_args_sum(null)", INTEGER, null);
        assertFunction("var_args_sum(1, null, 2, null, 3)", INTEGER, null);
        assertFunction("var_args_sum(1, 2, 3)", INTEGER, 6);

        // var_args_sum(1, 2, 3, ..., k)
        int k = 100;
        int expectedSum = (1 + k) * k / 2;
        assertFunction(format("var_args_sum(%s)", Joiner.on(",").join(IntStream.rangeClosed(1, k).boxed().collect(toSet()))), INTEGER, expectedSum);
    }

    public static class TestVarArgsSum
            extends SqlScalarFunction
    {
        public static final TestVarArgsSum VAR_ARGS_SUM = new TestVarArgsSum();

        private static final MethodHandle METHOD_HANDLE = methodHandle(TestVarArgsSum.class, "varArgsSum", Object.class, long[].class);
        private static final MethodHandle USER_STATE_FACTORY = methodHandle(TestVarArgsSum.class, "createState");

        private TestVarArgsSum()
        {
            super(new Signature(
                    "var_args_sum",
                    FunctionKind.SCALAR,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    INTEGER.getTypeSignature(),
                    ImmutableList.of(INTEGER.getTypeSignature()),
                    true));
        }

        @Override
        public boolean isHidden()
        {
            return false;
        }

        @Override
        public boolean isDeterministic()
        {
            return false;
        }

        @Override
        public String getDescription()
        {
            return "return sum of all the parameters";
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                    long.class,
                    long.class,
                    arity,
                    METHOD_HANDLE,
                    USER_STATE_FACTORY);
            return new ScalarFunctionImplementation(
                    false,
                    nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandleAndConstructor.getMethodHandle(),
                    Optional.of(methodHandleAndConstructor.getConstructor()),
                    isDeterministic());
        }

        @UsedByGeneratedCode
        public static Object createState()
        {
            return null;
        }

        @UsedByGeneratedCode
        public static long varArgsSum(Object state, long[] values)
        {
            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return sum;
        }
    }
}
