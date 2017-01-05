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

package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TypeSignatureProvider
{
    // hasDependency field exists primarily to make manipulating types without dependencies easy,
    // and to make toString more friendly.
    private final boolean hasDependency;
    private final Function<List<Type>, TypeSignature> typeSignatureResolver;

    public TypeSignatureProvider(TypeSignature typeSignature)
    {
        this.hasDependency = false;
        this.typeSignatureResolver = ignored -> typeSignature;
    }

    public TypeSignatureProvider(Function<List<Type>, TypeSignature> typeSignatureResolver)
    {
        this.hasDependency = true;
        this.typeSignatureResolver = requireNonNull(typeSignatureResolver, "typeSignatureResolver is null");
    }

    public boolean hasDependency()
    {
        return hasDependency;
    }

    public TypeSignature getTypeSignature()
    {
        checkState(!hasDependency);
        return typeSignatureResolver.apply(ImmutableList.of());
    }

    public TypeSignature getTypeSignature(List<Type> boundTypeParameters)
    {
        checkState(hasDependency);
        return typeSignatureResolver.apply(boundTypeParameters);
    }

    public static List<TypeSignatureProvider> fromTypes(List<? extends Type> types)
    {
        return types.stream()
                .map(Type::getTypeSignature)
                .map(TypeSignatureProvider::new)
                .collect(toImmutableList());
    }

    public static List<TypeSignatureProvider> fromTypeSignatures(List<? extends TypeSignature> typeSignatures)
    {
        return typeSignatures.stream()
                .map(TypeSignatureProvider::new)
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        if (hasDependency) {
            return super.toString();
        }
        return getTypeSignature().toString();
    }
}
