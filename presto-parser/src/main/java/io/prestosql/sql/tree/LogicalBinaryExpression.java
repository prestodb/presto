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

import static java.util.Objects.requireNonNull;

public class LogicalBinaryExpression
        extends Expression
{
    public enum Operator
    {
        AND, OR;

        public Operator flip()
        {
            switch (this) {
                case AND:
                    return OR;
                case OR:
                    return AND;
                default:
                    throw new IllegalArgumentException("Unsupported logical expression type: " + this);
            }
        }
    }

    private final Operator operator;
    private final Expression left;
    private final Expression right;

    public LogicalBinaryExpression(Operator operator, Expression left, Expression right)
    {
        this(Optional.empty(), operator, left, right);
    }

    public LogicalBinaryExpression(NodeLocation location, Operator operator, Expression left, Expression right)
    {
        this(Optional.of(location), operator, left, right);
    }

    private LogicalBinaryExpression(Optional<NodeLocation> location, Operator operator, Expression left, Expression right)
    {
        super(location);
        requireNonNull(operator, "operator is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogicalBinaryExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(left, right);
    }

    public static LogicalBinaryExpression and(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(Optional.empty(), Operator.AND, left, right);
    }

    public static LogicalBinaryExpression or(Expression left, Expression right)
    {
        return new LogicalBinaryExpression(Optional.empty(), Operator.OR, left, right);
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

        LogicalBinaryExpression that = (LogicalBinaryExpression) o;
        return operator == that.operator &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, left, right);
    }
}
