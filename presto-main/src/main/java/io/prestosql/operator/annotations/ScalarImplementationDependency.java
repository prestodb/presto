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
package io.prestosql.operator.annotations;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.Optional;

import static io.prestosql.metadata.SignatureBinder.applyBoundVariables;
import static java.util.Objects.requireNonNull;

public abstract class ScalarImplementationDependency
        implements ImplementationDependency
{
    private final Signature signature;
    private final Optional<InvocationConvention> invocationConvention;

    protected ScalarImplementationDependency(Signature signature, Optional<InvocationConvention> invocationConvention)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.invocationConvention = invocationConvention;
    }

    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public MethodHandle resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Signature signature = applyBoundVariables(this.signature, boundVariables, this.signature.getArgumentTypes().size());
        if (invocationConvention.isPresent()) {
            return functionRegistry.getFunctionInvokerProvider().createFunctionInvoker(signature, invocationConvention).methodHandle();
        }
        else {
            return functionRegistry.getScalarFunctionImplementation(signature).getMethodHandle();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScalarImplementationDependency that = (ScalarImplementationDependency) o;
        return Objects.equals(signature, that.signature) &&
                Objects.equals(invocationConvention, that.invocationConvention);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, invocationConvention);
    }
}
