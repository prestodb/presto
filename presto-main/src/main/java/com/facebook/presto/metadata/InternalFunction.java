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

import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class InternalFunction
{
    private InternalFunction()
    {
    }

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(ImmutableList.toImmutableList()));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalOperator(operator, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(operator.name()), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(FunctionRegistry.mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, FunctionKind.SCALAR, ImmutableList.of(), ImmutableList.of(), returnType, argumentTypes, false);
    }

    public static SignatureBuilder builder()
    {
        return new SignatureBuilder();
    }
}
