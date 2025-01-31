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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NativeFunctionHandle
        extends SqlFunctionHandle
{
    private final Signature signature;

    @JsonCreator
    public NativeFunctionHandle(@JsonProperty("signature") Signature signature)
    {
        // Todo: hardcoding version as "1"
        super(new SqlFunctionId(signature.getName(), signature.getArgumentTypes()), "1");
        this.signature = requireNonNull(signature, "signature is null");
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public String getName()
    {
        return signature.getName().toString();
    }

    @Override
    public FunctionKind getKind()
    {
        return signature.getKind();
    }

    @Override
    public CatalogSchemaName getCatalogSchemaName()
    {
        return signature.getName().getCatalogSchemaName();
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
        NativeFunctionHandle that = (NativeFunctionHandle) o;
        return Objects.equals(signature, that.signature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature);
    }

    @Override
    public String toString()
    {
        return signature.toString();
    }

    public static class Resolver
            implements FunctionHandleResolver
    {
        @Override
        public Class<? extends FunctionHandle> getFunctionHandleClass()
        {
            return NativeFunctionHandle.class;
        }
    }
}
