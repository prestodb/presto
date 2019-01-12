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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SimpleGroupBy
        extends GroupingElement
{
    private final List<Expression> columns;

    public SimpleGroupBy(List<Expression> simpleGroupByExpressions)
    {
        this(Optional.empty(), simpleGroupByExpressions);
    }

    public SimpleGroupBy(NodeLocation location, List<Expression> simpleGroupByExpressions)
    {
        this(Optional.of(location), simpleGroupByExpressions);
    }

    private SimpleGroupBy(Optional<NodeLocation> location, List<Expression> simpleGroupByExpressions)
    {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(simpleGroupByExpressions, "simpleGroupByExpressions is null"));
    }

    @Override
    public List<Expression> getExpressions()
    {
        return columns;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSimpleGroupBy(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return columns;
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
        SimpleGroupBy that = (SimpleGroupBy) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }
}
