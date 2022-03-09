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
package com.facebook.presto.sql.gen;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.gen.TestVarArgsToArrayAdapterGenerator.TestVarArgsSum.VAR_ARGS_SUM;
import static com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static com.facebook.presto.util.Reflection.methodHandle;
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

        private static final MethodHandle METHOD_HANDLE = methodHandle(TestVarArgsSum.class, "varArgsSum", long[].class);
        private TestVarArgsSum()
        {
            super(new Signature(
                    QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "var_args_sum"),
                    FunctionKind.SCALAR,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    INTEGER.getTypeSignature(),
                    ImmutableList.of(INTEGER.getTypeSignature()),
                    true));
        }

        @Override
        public SqlFunctionVisibility getVisibility()
        {
            return PUBLIC;
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
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            VarArgsToArrayAdapterGenerator.VarArgMethodHandle varArgMethodHandle = generateVarArgsToArrayAdapter(
                    long.class,
                    long.class,
                    arity,
                    METHOD_HANDLE);
            return new BuiltInScalarFunctionImplementation(
                    false,
                    nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    varArgMethodHandle.getMethodHandle());
        }

        @UsedByGeneratedCode
        public static long varArgsSum(long[] values)
        {
            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return sum;
        }
    }
}
