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

import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.util.function.Function;

import static com.facebook.presto.sql.gen.InvokeFunctionByteCodeExpression.invokeFunction;

public final class ArrayGeneratorUtils
{
    private ArrayGeneratorUtils()
    {
    }

    public static ArrayMapByteCodeExpression map(Scope scope, CallSiteBinder binder, TypeManager typeManager, Variable array, FunctionInfo elementFunction)
    {
        return map(
                scope,
                binder,
                typeManager,
                array,
                elementFunction.getSignature(),
                element -> invokeFunction(scope, binder, elementFunction, element));
    }

    public static ArrayMapByteCodeExpression map(
            Scope scope,
            CallSiteBinder binder,
            TypeManager typeManager,
            ByteCodeExpression array,
            Signature mapperSignature,
            Function<ByteCodeExpression, ByteCodeExpression> mapper)
    {
        Type fromElementType = typeManager.getType(mapperSignature.getArgumentTypes().get(0));
        Type toElementType = typeManager.getType(mapperSignature.getReturnType());
        return map(scope, binder, array, fromElementType, toElementType, mapper);
    }

    public static ArrayMapByteCodeExpression map(
            Scope scope,
            CallSiteBinder binder,
            ByteCodeExpression array,
            Type fromElementType,
            Type toElementType,
            Function<ByteCodeExpression, ByteCodeExpression> mapper)
    {
        return new ArrayMapByteCodeExpression(scope, binder, array, fromElementType, toElementType, mapper);
    }
}
