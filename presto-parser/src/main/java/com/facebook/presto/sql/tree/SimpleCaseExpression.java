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

import static java.util.Objects.requireNonNull;

public class SimpleCaseExpression
        extends Expression
{
    private final Expression operand;
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;

    public SimpleCaseExpression(Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        this(Optional.empty(), operand, whenClauses, defaultValue);
    }

    public SimpleCaseExpression(NodeLocation location, Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        this(Optional.of(location), operand, whenClauses, defaultValue);
    }

    private SimpleCaseExpression(Optional<NodeLocation> location, Expression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        super(location);
        requireNonNull(operand, "operand is null");
        requireNonNull(whenClauses, "whenClauses is null");

        this.operand = operand;
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public Expression getOperand()
    {
        return operand;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSimpleCaseExpression(this, context);
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

        SimpleCaseExpression that = (SimpleCaseExpression) o;
        return Objects.equals(operand, that.operand) &&
                Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operand, whenClauses, defaultValue);
    }
}
