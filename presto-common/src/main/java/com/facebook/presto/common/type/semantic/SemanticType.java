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
package com.facebook.presto.common.type.semantic;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public abstract class SemanticType
        implements Type
{
    private final SemanticTypeCategory category;
    private final TypeSignature typeSignature;

    SemanticType(SemanticTypeCategory category, TypeSignature typeSignature)
    {
        this.category = requireNonNull(category, "category is null");
        this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
    }

    public abstract String getName();
    public abstract Type getType();

    public static SemanticType from(QualifiedObjectName name, Type type)
    {
        return new DistinctType(name, type);
    }

    @Override
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @Override
    public String toString()
    {
        return typeSignature.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SemanticType other = (SemanticType) obj;

        return Objects.equals(this.category, other.category) &&
                Objects.equals(this.typeSignature, other.typeSignature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(category, typeSignature);
    }

    public enum SemanticTypeCategory
    {
        BUILTIN_TYPE,
        DISTINCT_TYPE,
        STRUCTURED_TYPE
    }
}
