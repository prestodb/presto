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

import com.facebook.presto.common.SourceLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class Lateral
        extends Relation
{
    private final Query query;

    public Lateral(Query query)
    {
        this(Optional.empty(), query);
    }

    public Lateral(SourceLocation location, Query query)
    {
        this(Optional.of(location), query);
    }

    private Lateral(Optional<SourceLocation> location, Query query)
    {
        super(location);
        this.query = requireNonNull(query, "query is null");
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLateral(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(query);
    }

    @Override
    public String toString()
    {
        return "LATERAL(" + query + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query);
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
        Lateral other = (Lateral) obj;
        return Objects.equals(this.query, other.query);
    }
}
