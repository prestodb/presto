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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PlainTypeSignatureProvider
        implements TypeSignatureProvider
{
    private final TypeSignature typeSignature;

    public PlainTypeSignatureProvider(TypeSignature typeSignature)
    {
        this.typeSignature = requireNonNull(typeSignature);
    }

    public static List<PlainTypeSignatureProvider> fromTypes(List<? extends Type> types)
    {
        return types.stream()
                .map(Type::getTypeSignature)
                .map(PlainTypeSignatureProvider::new)
                .collect(toImmutableList());
    }

    public static List<PlainTypeSignatureProvider> fromTypeSignatures(List<? extends TypeSignature> typeSignatures)
    {
        return typeSignatures.stream()
                .map(PlainTypeSignatureProvider::new)
                .collect(toImmutableList());
    }

    @Override
    public boolean hasDependency()
    {
        return false;
    }

    @Override
    public TypeSignature getTypeSignature(List<Type> boundTypeParameters)
    {
        checkArgument(boundTypeParameters.isEmpty(), "Independent type signature doesn't take any type parameters.");
        return typeSignature;
    }

    public TypeSignature getTypeSignature()
    {
        return getTypeSignature(ImmutableList.of());
    }

    @Override
    public String toString()
    {
        return getTypeSignature(ImmutableList.of()).toString();
    }
}
