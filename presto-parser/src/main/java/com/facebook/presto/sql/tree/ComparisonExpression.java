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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ComparisonExpression
        extends Expression
{
    private final ComparisonExpressionType type;
    private final Expression left;
    private final Expression right;

    public ComparisonExpression(ComparisonExpressionType type, Expression left, Expression right)
    {
        this(Optional.empty(), type, left, right);
    }

    public ComparisonExpression(NodeLocation location, ComparisonExpressionType type, Expression left, Expression right)
    {
        this(Optional.of(location), type, left, right);
    }

    private ComparisonExpression(Optional<NodeLocation> location, ComparisonExpressionType type, Expression left, Expression right)
    {
        super(location);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.type = type;
        this.left = left;
        this.right = right;
    }

    public ComparisonExpressionType getType()
    {
        return type;
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
        return visitor.visitComparisonExpression(this, context);
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

        ComparisonExpression that = (ComparisonExpression) o;
        return (type == that.type) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, left, right);
    }
}
