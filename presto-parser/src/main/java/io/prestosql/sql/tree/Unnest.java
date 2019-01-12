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

public final class Unnest
        extends Relation
{
    private final List<Expression> expressions;
    private final boolean withOrdinality;

    public Unnest(List<Expression> expressions, boolean withOrdinality)
    {
        this(Optional.empty(), expressions, withOrdinality);
    }

    public Unnest(NodeLocation location, List<Expression> expressions, boolean withOrdinality)
    {
        this(Optional.of(location), expressions, withOrdinality);
    }

    private Unnest(Optional<NodeLocation> location, List<Expression> expressions, boolean withOrdinality)
    {
        super(location);
        requireNonNull(expressions, "expressions is null");
        this.expressions = ImmutableList.copyOf(expressions);
        this.withOrdinality = withOrdinality;
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    public boolean isWithOrdinality()
    {
        return withOrdinality;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return expressions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expressions", expressions)
                .add("withOrdinality", withOrdinality)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expressions, withOrdinality);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Unnest other = (Unnest) obj;
        return Objects.equals(expressions, other.expressions) && withOrdinality == other.withOrdinality;
    }
}
