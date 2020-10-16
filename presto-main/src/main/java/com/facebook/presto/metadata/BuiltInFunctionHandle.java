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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.Signature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BuiltInFunctionHandle
        implements FunctionHandle
{
    private final Signature signature;

    @JsonCreator
    public BuiltInFunctionHandle(@JsonProperty("signature") Signature signature)
    {
        this.signature = requireNonNull(signature, "signature is null");
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
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
        BuiltInFunctionHandle that = (BuiltInFunctionHandle) o;
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

    private static void checkArgument(boolean condition, String message, Object... args)
    {
        if (!condition) {
            throw new IllegalArgumentException(String.format(message, args));
        }
    }
}
