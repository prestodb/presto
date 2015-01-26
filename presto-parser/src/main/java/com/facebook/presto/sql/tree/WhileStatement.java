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

public class WhileStatement
        extends ControlStatement
{
    private final Optional<String> label;
    private final Expression expression;
    private final List<Statement> statements;

    public WhileStatement(Optional<String> label, Expression expression, List<Statement> statements)
    {
        this(Optional.empty(), label, expression, statements);
    }

    public WhileStatement(NodeLocation location, Optional<String> label, Expression expression, List<Statement> statements)
    {
        this(Optional.of(location), label, expression, statements);
    }

    public WhileStatement(Optional<NodeLocation> location, Optional<String> label, Expression expression, List<Statement> statements)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.statements = requireNonNull(statements, "statements is null");
    }

    public Optional<String> getLabel()
    {
        return label;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWhileStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, expression, statements);
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
        WhileStatement o = (WhileStatement) obj;
        return Objects.equals(label, o.label) &&
                Objects.equals(expression, o.expression) &&
                Objects.equals(statements, o.statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .add("expression", expression)
                .add("statements", statements)
                .toString();
    }
}
