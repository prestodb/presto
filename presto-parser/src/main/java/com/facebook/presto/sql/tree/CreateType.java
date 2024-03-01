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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class CreateType
        extends Statement
{
    private final QualifiedName typeName;
    private final Optional<String> distinctType;
    private final List<String> parameterNames;
    private final List<String> parameterTypes;

    public CreateType(QualifiedName typeName, String distinctType)
    {
        this(typeName, Optional.of(distinctType), ImmutableList.of(), ImmutableList.of());
    }

    public CreateType(QualifiedName typeName, ImmutableList<String> parameterNames, ImmutableList<String> parameterTypes)
    {
        this(typeName, Optional.empty(), parameterNames, parameterTypes);
    }

    private CreateType(QualifiedName typeName, Optional<String> distinctType, ImmutableList<String> parameterNames, ImmutableList<String> parameterTypes)
    {
        super(Optional.empty());

        checkArgument(parameterNames.size() == parameterTypes.size(), "Expected one type for each parameter name");
        checkArgument(distinctType.isPresent() || !parameterNames.isEmpty(), "Type can be either a distinct type or parameterized");

        this.typeName = typeName;
        this.distinctType = distinctType;
        this.parameterNames = parameterNames;
        this.parameterTypes = parameterTypes;

        // Check for duplicate fields
        HashSet<String> parameterNamesSet = new HashSet();
        parameterNames.forEach(parameterName -> {
            checkArgument(!parameterNamesSet.contains(parameterName), format("Duplicated member definition for %s in %s", parameterName, typeName));
            parameterNamesSet.add(parameterName);
        });
    }

    public QualifiedName getTypeName()
    {
        return typeName;
    }

    public Optional<String> getDistinctType()
    {
        return distinctType;
    }

    public List<String> getParameterNames()
    {
        return parameterNames;
    }

    public List<String> getParameterTypes()
    {
        return parameterTypes;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateType(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeName, distinctType, parameterNames, parameterTypes);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        CreateType o = (CreateType) obj;

        return Objects.equals(typeName, o.typeName) &&
                Objects.equals(distinctType, o.distinctType) &&
                Objects.equals(parameterNames, o.parameterNames) &&
                Objects.equals(parameterTypes, o.parameterTypes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("typeName", typeName)
                .add("distinctType", distinctType)
                .add("parameterNames", parameterNames)
                .add("parameterTypes", parameterTypes)
                .toString();
    }
}
