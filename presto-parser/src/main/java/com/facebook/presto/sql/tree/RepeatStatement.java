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

public class RepeatStatement
        extends ControlStatement
{
    private final Optional<String> label;
    private final List<Statement> statements;
    private final Expression condition;

    public RepeatStatement(Optional<String> label, List<Statement> statements, Expression condition)
    {
        this(Optional.empty(), label, statements, condition);
    }

    public RepeatStatement(NodeLocation location, Optional<String> label, List<Statement> statements, Expression condition)
    {
        this(Optional.of(location), label, statements, condition);
    }

    public RepeatStatement(Optional<NodeLocation> location, Optional<String> label, List<Statement> statements, Expression condition)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
        this.statements = requireNonNull(statements, "statements is null");
        this.condition = requireNonNull(condition, "condition is null");
    }

    public Optional<String> getLabel()
    {
        return label;
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    public Expression getCondition()
    {
        return condition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRepeatStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, statements, condition);
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
        RepeatStatement o = (RepeatStatement) obj;
        return Objects.equals(label, o.label) &&
                Objects.equals(statements, o.statements) &&
                Objects.equals(condition, o.condition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .add("statements", statements)
                .add("condition", condition)
                .toString();
    }
}
