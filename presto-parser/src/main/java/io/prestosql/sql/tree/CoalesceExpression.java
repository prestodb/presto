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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CoalesceExpression
        extends Expression
{
    private final List<Expression> operands;

    public CoalesceExpression(Expression first, Expression second, Expression... additional)
    {
        this(Optional.empty(), ImmutableList.<Expression>builder()
                .add(first, second)
                .add(additional)
                .build());
    }

    public CoalesceExpression(List<Expression> operands)
    {
        this(Optional.empty(), operands);
    }

    public CoalesceExpression(NodeLocation location, List<Expression> operands)
    {
        this(Optional.of(location), operands);
    }

    private CoalesceExpression(Optional<NodeLocation> location, List<Expression> operands)
    {
        super(location);
        requireNonNull(operands, "operands is null");
        checkArgument(operands.size() >= 2, "must have at least two operands");

        this.operands = ImmutableList.copyOf(operands);
    }

    public List<Expression> getOperands()
    {
        return operands;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCoalesceExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return operands;
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

        CoalesceExpression that = (CoalesceExpression) o;
        return Objects.equals(operands, that.operands);
    }

    @Override
    public int hashCode()
    {
        return operands.hashCode();
    }
}
