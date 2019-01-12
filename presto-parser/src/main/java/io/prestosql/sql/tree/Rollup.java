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

public final class Rollup
        extends GroupingElement
{
    private final List<Expression> columns;

    public Rollup(List<Expression> columns)
    {
        this(Optional.empty(), columns);
    }

    public Rollup(NodeLocation location, List<Expression> columns)
    {
        this(Optional.of(location), columns);
    }

    private Rollup(Optional<NodeLocation> location, List<Expression> columns)
    {
        super(location);
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @Override
    public List<Expression> getExpressions()
    {
        return columns;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRollup(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        Rollup rollup = (Rollup) o;
        return Objects.equals(columns, rollup.columns);
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
