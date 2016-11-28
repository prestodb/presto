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

public class Format
        extends Statement
{
    private final Statement statement;

    public Format(Statement statement)
    {
        this(Optional.empty(), statement);
    }

    public Format(NodeLocation location, Statement statement)
    {
        this(Optional.of(location), statement);
    }

    private Format(Optional<NodeLocation> location, Statement statement)
    {
        super(location);
        this.statement = requireNonNull(statement, "statement is null");
    }

    public Statement getStatement()
    {
        return statement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFormat(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statement);
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
        Format o = (Format) obj;
        return Objects.equals(statement, o.statement);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statement", statement)
                .toString();
    }
}
