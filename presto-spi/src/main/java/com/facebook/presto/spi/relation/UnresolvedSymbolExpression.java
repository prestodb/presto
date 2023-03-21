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
import com.facebook.presto.spi.SourceLocation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class UnresolvedSymbolExpression
        extends IntermediateFormExpression
{
    private final Type type;
    private final List<String> name;

    public UnresolvedSymbolExpression(Optional<SourceLocation> sourceLocation, Type type, List<String> name)
    {
        super(sourceLocation);
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null");
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public List<RowExpression> getChildren()
    {
        return emptyList();
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
        UnresolvedSymbolExpression other = (UnresolvedSymbolExpression) obj;
        return Objects.equals(this.type, other.type) &&
                Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return format("UnresolvedSymbolExpression{%s, %s}", type, name);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnresolvedSymbolExpression(this, context);
    }

    @Override
    public RowExpression canonicalize()
    {
        return this;
    }
}
