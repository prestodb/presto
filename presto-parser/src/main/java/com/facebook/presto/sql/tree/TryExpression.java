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

import com.facebook.presto.common.SourceLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TryExpression
        extends Expression
{
    private final Expression innerExpression;

    public TryExpression(Expression innerExpression)
    {
        this(Optional.empty(), innerExpression);
    }

    public TryExpression(SourceLocation location, Expression innerExpression)
    {
        this(Optional.of(location), innerExpression);
    }

    private TryExpression(Optional<SourceLocation> location, Expression innerExpression)
    {
        super(location);
        this.innerExpression = requireNonNull(innerExpression, "innerExpression is null");
    }

    public Expression getInnerExpression()
    {
        return innerExpression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTryExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(innerExpression);
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
        TryExpression o = (TryExpression) obj;
        return Objects.equals(innerExpression, o.innerExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(innerExpression);
    }
}
