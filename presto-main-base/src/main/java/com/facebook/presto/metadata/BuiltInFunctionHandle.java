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
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInFunctionKind.ENGINE;
import static java.util.Objects.requireNonNull;

public class BuiltInFunctionHandle
        implements FunctionHandle
{
    private final Signature signature;
    private final BuiltInFunctionKind builtInFunctionKind;

    public BuiltInFunctionHandle(Signature signature)
    {
        this(signature, ENGINE);
    }

    @JsonCreator
    public BuiltInFunctionHandle(@JsonProperty("signature") Signature signature, @JsonProperty("builtInFunctionKind") BuiltInFunctionKind builtInFunctionKind)
    {
        this.signature = requireNonNull(signature, "signature is null");
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        this.builtInFunctionKind = requireNonNull(builtInFunctionKind, "builtInFunctionKind is null");
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
    public List<TypeSignature> getArgumentTypes()
    {
        return signature.getArgumentTypes();
    }

    @Override
    public Optional<TypeSignature> getReturnType()
    {
        return Optional.of(getSignature().getReturnType());
    }

    @Override
    public CatalogSchemaName getCatalogSchemaName()
    {
        return signature.getName().getCatalogSchemaName();
    }

    @JsonProperty
    public BuiltInFunctionKind getBuiltInFunctionKind()
    {
        return builtInFunctionKind;
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
        return Objects.equals(signature, that.signature)
                && Objects.equals(builtInFunctionKind, that.builtInFunctionKind);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, builtInFunctionKind);
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

    /*
     * Two instances of same functions can appear different when types have params, for example a function with varchar return type may not match with instance of
     * same function but with return type for example varchar(1). Here canonicalize will erase any type params, just for the purpose of correct matching of two functionHandle.
     */
    @Override
    public BuiltInFunctionHandle canonicalize()
    {
        List<TypeSignature> arguments = signature.getArgumentTypes().stream().map(type -> parseTypeSignature(type.getBase())).collect(Collectors.toList());
        SignatureBuilder signatureBuilder = new SignatureBuilder().from(signature).argumentTypes(arguments).returnType(parseTypeSignature(signature.getReturnType().getBase()));
        return new BuiltInFunctionHandle(signatureBuilder.build(), this.builtInFunctionKind);
    }
}
