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
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.util.Objects;

import static io.prestosql.metadata.SignatureBinder.applyBoundVariables;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public final class TypeImplementationDependency
        implements ImplementationDependency
{
    private final TypeSignature signature;

    public TypeImplementationDependency(String signature)
    {
        this.signature = parseTypeSignature(requireNonNull(signature, "signature is null"));
    }

    @Override
    public Type resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return typeManager.getType(applyBoundVariables(signature, boundVariables));
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
        TypeImplementationDependency that = (TypeImplementationDependency) o;
        return Objects.equals(signature, that.signature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature);
    }
}
