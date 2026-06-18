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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Return
        extends RoutineBody
{
    private final Expression expression;

    public Return(Expression expression)
    {
        this(Optional.empty(), expression);
    }

    public Return(NodeLocation location, Expression expression)
    {
        this(Optional.of(location), expression);
    }

    private Return(Optional<NodeLocation> location, Expression expression)
    {
        super(location);
        this.expression = requireNonNull(expression, "Expression is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturn(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression);
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
        Return o = (Return) obj;
        return Objects.equals(expression, o.expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .toString();
    }
}
