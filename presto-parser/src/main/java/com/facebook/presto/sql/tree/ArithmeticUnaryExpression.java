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

import static com.google.common.base.Preconditions.checkNotNull;

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
        checkNotNull(value, "value is null");
        checkNotNull(sign, "sign is null");

        this.value = value;
        this.sign = sign;
    }

    public static ArithmeticUnaryExpression positive(Expression value)
    {
        return new ArithmeticUnaryExpression(Sign.PLUS, value);
    }

    public static ArithmeticUnaryExpression negative(Expression value)
    {
        return new ArithmeticUnaryExpression(Sign.MINUS, value);
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;

        if (sign != that.sign) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = value.hashCode();
        result = 31 * result + sign.hashCode();
        return result;
    }
}
