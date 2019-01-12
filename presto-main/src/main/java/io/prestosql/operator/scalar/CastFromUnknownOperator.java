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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public final class CastFromUnknownOperator
        extends SqlOperator
{
    public static final CastFromUnknownOperator CAST_FROM_UNKNOWN = new CastFromUnknownOperator();
    private static final MethodHandle METHOD_HANDLE_NON_NULL = methodHandle(CastFromUnknownOperator.class, "handleNonNull", boolean.class);

    public CastFromUnknownOperator()
    {
        super(CAST,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("unknown")));
    }

    @Override
    public ScalarFunctionImplementation specialize(
            BoundVariables boundVariables, int arity, TypeManager typeManager,
            FunctionRegistry functionRegistry)
    {
        Type toType = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle = METHOD_HANDLE_NON_NULL.asType(METHOD_HANDLE_NON_NULL.type().changeReturnType(toType.getJavaType()));
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object handleNonNull(boolean arg)
    {
        throw new IllegalArgumentException("value of unknown type should always be null");
    }
}
