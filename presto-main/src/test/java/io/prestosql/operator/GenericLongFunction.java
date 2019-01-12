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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.function.LongUnaryOperator;

import static io.prestosql.metadata.Signature.internalScalarFunction;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public final class GenericLongFunction
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(GenericLongFunction.class, "apply", LongUnaryOperator.class, long.class);

    private final LongUnaryOperator longUnaryOperator;

    GenericLongFunction(String suffix, LongUnaryOperator longUnaryOperator)
    {
        super(internalScalarFunction("generic_long_" + requireNonNull(suffix, "suffix is null"), parseTypeSignature(BIGINT), parseTypeSignature(BIGINT)));
        this.longUnaryOperator = longUnaryOperator;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "generic long function for test";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(longUnaryOperator);
        return new ScalarFunctionImplementation(false, ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)), methodHandle, isDeterministic());
    }

    public static long apply(LongUnaryOperator longUnaryOperator, long value)
    {
        return longUnaryOperator.applyAsLong(value);
    }
}
