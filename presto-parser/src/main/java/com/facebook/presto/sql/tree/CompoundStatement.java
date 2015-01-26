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
import static com.google.common.base.Preconditions.checkNotNull;

public class CompoundStatement
        extends ControlStatement
{
    private final Optional<String> label;
    private final List<VariableDeclaration> variableDeclarations;
    private final List<Statement> statements;

    public CompoundStatement(
            Optional<String> label,
            List<VariableDeclaration> variableDeclarations,
            List<Statement> statements)
    {
        this.label = checkNotNull(label, "label is null");
        this.variableDeclarations = checkNotNull(variableDeclarations, "variableDeclarations is null");
        this.statements = checkNotNull(statements, "statements is null");
    }

    public Optional<String> getLabel()
    {
        return label;
    }

    public List<Statement> getStatements()
    {
        return statements;
    }

    public List<VariableDeclaration> getVariableDeclarations()
    {
        return variableDeclarations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCompoundStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, variableDeclarations, statements);
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
        CompoundStatement o = (CompoundStatement) obj;
        return Objects.equals(label, o.label) &&
                Objects.equals(variableDeclarations, o.variableDeclarations) &&
                Objects.equals(statements, o.statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .add("variableDeclarations", variableDeclarations)
                .add("statements", statements)
                .toString();
    }
}
