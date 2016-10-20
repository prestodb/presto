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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FilterClause
        extends Node
{
    private final Expression filterClause;

    public FilterClause(Optional<Expression> filterClause)
    {
        this(Optional.empty(), filterClause);
    }

    public FilterClause(NodeLocation location, Optional<Expression> filterClause)
    {
        this(Optional.of(location), filterClause);
    }

    private FilterClause(Optional<NodeLocation> location, Optional<Expression> filterClause)
    {
        super(location);
        this.filterClause = requireNonNull(filterClause.get(), "filter where clause is null");
    }

    public Expression getFilterClause()
    {
        return this.filterClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilterClause(this, context);
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

        FilterClause filter1 = (FilterClause) o;

        return filterClause.equals(filter1.filterClause);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(filterClause);
    }
    @Override

    public String toString()
    {
        return toStringHelper(this)
                .add("filterClause", filterClause)
                .toString();
    }
}
