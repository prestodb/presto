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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.experimental.auto_gen.ThriftDistinctTypeInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class DistinctTypeInfo
{
    private final QualifiedObjectName name;
    private final TypeSignature baseType;
    // Ancestor is either parent of this type, or one of the recursive parents.
    private final Optional<QualifiedObjectName> topMostAncestor;
    // This contains all ancestors except the topmost ancestor.
    private final List<QualifiedObjectName> otherAncestors;
    private final boolean isOrderable;

    public DistinctTypeInfo(ThriftDistinctTypeInfo thriftDistinctTypeInfo)
    {
        this(new QualifiedObjectName(thriftDistinctTypeInfo.getName()),
                new TypeSignature(thriftDistinctTypeInfo.getBaseType()),
                Optional.ofNullable(thriftDistinctTypeInfo.getTopMostAncestor()).map(QualifiedObjectName::new),
                thriftDistinctTypeInfo.getOtherAncestors().stream().map(QualifiedObjectName::new).collect(Collectors.toList()),
                thriftDistinctTypeInfo.isIsOrderable());
    }

    public ThriftDistinctTypeInfo toThrift()
    {
        return new ThriftDistinctTypeInfo(
                this.name.toThrift(),
                this.baseType.toThrift(),
                this.isOrderable,
                topMostAncestor.map(QualifiedObjectName::toThrift).orElse(null),
                this.otherAncestors.stream().map(QualifiedObjectName::toThrift).collect(Collectors.toList()));
    }

    @ThriftConstructor
    @JsonCreator
    public DistinctTypeInfo(
            @JsonProperty("name") QualifiedObjectName name,
            @JsonProperty("baseType") TypeSignature baseType,
            @JsonProperty("lastAncestor") Optional<QualifiedObjectName> topMostAncestor,
            @JsonProperty("ancestors") List<QualifiedObjectName> otherAncestors,
            @JsonProperty("isOrderable") boolean isOrderable)
    {
        this.name = requireNonNull(name, "name is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.topMostAncestor = requireNonNull(topMostAncestor, "lastAncestor is null");
        this.otherAncestors = unmodifiableList(requireNonNull(otherAncestors, "otherAncestors is null"));
        this.isOrderable = isOrderable;
    }

    public DistinctTypeInfo(
            QualifiedObjectName name,
            TypeSignature baseType,
            Optional<QualifiedObjectName> parent,
            boolean isOrderable)
    {
        this(name, baseType, parent, emptyList(), isOrderable);
    }

    @ThriftField(1)
    @JsonProperty
    public QualifiedObjectName getName()
    {
        return name;
    }

    @ThriftField(2)
    @JsonProperty
    public TypeSignature getBaseType()
    {
        return baseType;
    }

    @ThriftField(3)
    @JsonProperty
    public List<QualifiedObjectName> getOtherAncestors()
    {
        return otherAncestors;
    }

    @ThriftField(4)
    @JsonProperty
    public Optional<QualifiedObjectName> getTopMostAncestor()
    {
        return topMostAncestor;
    }

    @JsonProperty
    public boolean isOrderable()
    {
        return isOrderable;
    }

    @ThriftField(5)
    public boolean getIsOrderable()
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
        return format(
                "%s{%s, %s, %s, %s}",
                name,
                baseType,
                isOrderable,
                topMostAncestor.isPresent() ? topMostAncestor.get().toString() : "null",
                otherAncestors);
    }
}
