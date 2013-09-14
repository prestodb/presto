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

import com.google.common.base.Preconditions;

public class BetweenPredicate
        extends Expression
{
    private final Expression value;
    private final Expression min;
    private final Expression max;

    public BetweenPredicate(Expression value, Expression min, Expression max)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkNotNull(min, "min is null");
        Preconditions.checkNotNull(max, "max is null");

        this.value = value;
        this.min = min;
        this.max = max;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getMin()
    {
        return min;
    }

    public Expression getMax()
    {
        return max;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenPredicate(this, context);
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

        BetweenPredicate that = (BetweenPredicate) o;

        if (!max.equals(that.max)) {
            return false;
        }
        if (!min.equals(that.min)) {
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
        result = 31 * result + min.hashCode();
        result = 31 * result + max.hashCode();
        return result;
    }
}
