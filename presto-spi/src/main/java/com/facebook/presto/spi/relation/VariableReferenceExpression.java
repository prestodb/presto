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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.semantic.SemanticType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class VariableReferenceExpression
        extends RowExpression
        implements Comparable<VariableReferenceExpression>
{
    private final String name;
    private final Type type;

    @JsonCreator
    public VariableReferenceExpression(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        if (type instanceof SemanticType) {
            return ((SemanticType) type).getType();
        }
        return type;
    }

    @Override
    public int hashCode()
    {
        // TODO: Changed to getType() during semantic type refactoring because. Switch back to type after done.
        return Objects.hash(name, getType());
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitVariableReference(this, context);
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
        VariableReferenceExpression other = (VariableReferenceExpression) obj;
        // TODO: Changed to getType() during semantic type refactoring because. Switch back to Objects.equals(this.type, other.type) after done.
        return Objects.equals(this.name, other.name) && Objects.equals(this.getType(), other.getType());
    }

    @Override
    public int compareTo(VariableReferenceExpression o)
    {
        int nameComparison = name.compareTo(o.name);
        if (nameComparison != 0) {
            return nameComparison;
        }
        return type.getTypeSignature().toString().compareTo(o.type.getTypeSignature().toString());
    }
}
