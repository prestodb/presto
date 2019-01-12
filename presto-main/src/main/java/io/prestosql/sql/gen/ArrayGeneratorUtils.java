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

import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.type.Type;

import java.util.function.Function;

import static io.prestosql.sql.gen.InvokeFunctionBytecodeExpression.invokeFunction;

public final class ArrayGeneratorUtils
{
    private ArrayGeneratorUtils()
    {
    }

    public static ArrayMapBytecodeExpression map(Scope scope, CachedInstanceBinder cachedInstanceBinder, Type fromElementType, Type toElementType, Variable array, String elementFunctionName, ScalarFunctionImplementation elementFunction)
    {
        return map(
                scope,
                cachedInstanceBinder.getCallSiteBinder(),
                fromElementType,
                toElementType,
                array,
                element -> invokeFunction(scope, cachedInstanceBinder, elementFunctionName, elementFunction, element));
    }

    public static ArrayMapBytecodeExpression map(
            Scope scope,
            CallSiteBinder binder,
            Type fromElementType,
            Type toElementType,
            BytecodeExpression array,
            Function<BytecodeExpression, BytecodeExpression> mapper)
    {
        return map(scope, binder, array, fromElementType, toElementType, mapper);
    }

    public static ArrayMapBytecodeExpression map(
            Scope scope,
            CallSiteBinder binder,
            BytecodeExpression array,
            Type fromElementType,
            Type toElementType,
            Function<BytecodeExpression, BytecodeExpression> mapper)
    {
        return new ArrayMapBytecodeExpression(scope, binder, array, fromElementType, toElementType, mapper);
    }
}
