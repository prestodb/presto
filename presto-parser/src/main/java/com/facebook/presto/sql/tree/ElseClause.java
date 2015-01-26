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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElseClause
        extends Node
{
    private final List<Statement> statements;

    public ElseClause(List<Statement> statements)
    {
        this(Optional.empty(), statements);
    }

    public ElseClause(NodeLocation location, List<Statement> statements)
    {
        this(Optional.of(location), statements);
    }

    public ElseClause(Optional<NodeLocation> location, List<Statement> statements)
    {
        super(location);
        this.statements = requireNonNull(statements, "statements is null");
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitElseClause(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statements);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ElseClause o = (ElseClause) obj;
        return Objects.equals(statements, o.statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statements", statements)
                .toString();
    }
}
