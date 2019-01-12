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

public class ArithmeticUnaryExpression
        extends Expression
{
    public enum Sign
    {
        PLUS,
        MINUS
    }

    private final Expression value;
    private final Sign sign;

    public ArithmeticUnaryExpression(Sign sign, Expression value)
    {
        this(Optional.empty(), sign, value);
    }

    public ArithmeticUnaryExpression(NodeLocation location, Sign sign, Expression value)
    {
        this(Optional.of(location), sign, value);
    }

    private ArithmeticUnaryExpression(Optional<NodeLocation> location, Sign sign, Expression value)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");

        this.value = value;
        this.sign = sign;
    }

    public static ArithmeticUnaryExpression positive(NodeLocation location, Expression value)
    {
        return new ArithmeticUnaryExpression(Optional.of(location), Sign.PLUS, value);
    }

    public static ArithmeticUnaryExpression negative(NodeLocation location, Expression value)
    {
        return new ArithmeticUnaryExpression(Optional.of(location), Sign.MINUS, value);
    }

    public static ArithmeticUnaryExpression positive(Expression value)
    {
        return new ArithmeticUnaryExpression(Optional.empty(), Sign.PLUS, value);
    }

    public static ArithmeticUnaryExpression negative(Expression value)
    {
        return new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, value);
    }

    public Expression getValue()
    {
        return value;
    }

    public Sign getSign()
    {
        return sign;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticUnary(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value);
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

        ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;
        return Objects.equals(value, that.value) &&
                (sign == that.sign);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, sign);
    }
}
