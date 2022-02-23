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
package com.facebook.presto.common.type;

import com.facebook.presto.common.QualifiedObjectName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DistinctTypeInfo
{
    private final QualifiedObjectName name;
    private final TypeSignature baseType;
    private final List<Optional<QualifiedObjectName>> ancestors;
    private final boolean isOrderable;

    @JsonCreator
    public DistinctTypeInfo(
            @JsonProperty("name") QualifiedObjectName name,
            @JsonProperty("baseType") TypeSignature baseType,
            @JsonProperty("parents") List<Optional<QualifiedObjectName>> ancestors,
            @JsonProperty("isOrderable") boolean isOrderable)
    {
        this.name = requireNonNull(name, "name is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.ancestors = unmodifiableList(requireNonNull(ancestors, "ancestors is null"));
        this.isOrderable = isOrderable;
    }

    public DistinctTypeInfo(QualifiedObjectName name, TypeSignature baseType, Optional<QualifiedObjectName> parent, boolean isOrderable)
    {
        this(name, baseType, singletonList(parent), isOrderable);
    }

    @JsonProperty
    public QualifiedObjectName getName()
    {
        return name;
    }

    @JsonProperty
    public TypeSignature getBaseType()
    {
        return baseType;
    }

    @JsonProperty
    public List<Optional<QualifiedObjectName>> getAncestors()
    {
        return ancestors;
    }

    public Optional<QualifiedObjectName> getParent()
    {
        return ancestors.get(0);
    }

    @JsonProperty
    public boolean isOrderable()
    {
        return isOrderable;
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

        DistinctTypeInfo other = (DistinctTypeInfo) o;

        return Objects.equals(name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public String toString()
    {
        return format("%s{%s, %s, %s}", name, baseType, isOrderable, ancestors.stream().map(name -> name.isPresent() ? name.get() : "null").collect(toList()));
    }
}
