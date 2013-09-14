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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class EquiJoinClause
{
    private final Expression left;
    private final Expression right;

    public EquiJoinClause(Expression left, Expression right)
    {
        Preconditions.checkNotNull(left, "left is null");
        Preconditions.checkNotNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }

    public static Function<EquiJoinClause, Expression> leftGetter()
    {
        return new Function<EquiJoinClause, Expression>()
        {
            @Override
            public Expression apply(EquiJoinClause input)
            {
                return input.getLeft();
            }
        };
    }

    public static Function<EquiJoinClause, Expression> rightGetter()
    {
        return new Function<EquiJoinClause, Expression>()
        {
            @Override
            public Expression apply(EquiJoinClause input)
            {
                return input.getRight();
            }
        };
    }
}
