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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class SqlScalarFunction
        extends BuiltInFunction
{
    private final Signature signature;

    protected SqlScalarFunction(Signature signature)
    {
        this.signature = requireNonNull(signature, "signature is null");
        checkArgument(signature.getKind() == SCALAR, "function kind must be SCALAR");
    }

    @Override
    public final Signature getSignature()
    {
        return signature;
    }

    public abstract BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager);

    public static PolymorphicScalarFunctionBuilder builder(Class<?> clazz, OperatorType operatorType)
    {
        return new PolymorphicScalarFunctionBuilder(clazz, operatorType);
    }

    public static PolymorphicScalarFunctionBuilder builder(Class<?> clazz)
    {
        return new PolymorphicScalarFunctionBuilder(clazz);
    }
}
