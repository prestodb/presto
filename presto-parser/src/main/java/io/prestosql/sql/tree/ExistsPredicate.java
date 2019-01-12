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

public class ExistsPredicate
        extends Expression
{
    private final Expression subquery;

    public ExistsPredicate(Expression subquery)
    {
        this(Optional.empty(), subquery);
    }

    public ExistsPredicate(NodeLocation location, Expression subquery)
    {
        this(Optional.of(location), subquery);
    }

    private ExistsPredicate(Optional<NodeLocation> location, Expression subquery)
    {
        super(location);
        requireNonNull(subquery, "subquery is null");
        this.subquery = subquery;
    }

    public Expression getSubquery()
    {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExists(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(subquery);
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

        ExistsPredicate that = (ExistsPredicate) o;
        return Objects.equals(subquery, that.subquery);
    }

    @Override
    public int hashCode()
    {
        return subquery.hashCode();
    }
}
