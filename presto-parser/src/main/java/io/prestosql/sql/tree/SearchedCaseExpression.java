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

public class SearchedCaseExpression
        extends Expression
{
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;

    public SearchedCaseExpression(List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        this(Optional.empty(), whenClauses, defaultValue);
    }

    public SearchedCaseExpression(NodeLocation location, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        this(Optional.of(location), whenClauses, defaultValue);
    }

    private SearchedCaseExpression(Optional<NodeLocation> location, List<WhenClause> whenClauses, Optional<Expression> defaultValue)
    {
        super(location);
        requireNonNull(whenClauses, "whenClauses is null");
        requireNonNull(defaultValue, "defaultValue is null");
        this.whenClauses = ImmutableList.copyOf(whenClauses);
        this.defaultValue = defaultValue;
    }

    public List<WhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSearchedCaseExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(whenClauses);
        defaultValue.ifPresent(nodes::add);
        return nodes.build();
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

        SearchedCaseExpression that = (SearchedCaseExpression) o;
        return Objects.equals(whenClauses, that.whenClauses) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whenClauses, defaultValue);
    }
}
