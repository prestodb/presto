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

import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.metadata.OperatorSignatureUtils.mangleOperatorName;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public final class InternalSignatureUtils
{
    private InternalSignatureUtils() {}

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toList()));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalOperator(operator, returnType, unmodifiableList(Arrays.asList(argumentTypes)));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, unmodifiableList(Arrays.asList(argumentTypes)));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, unmodifiableList(Arrays.asList(argumentTypes)));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, emptyList(), emptyList(), returnType, argumentTypes, false);
    }
}
