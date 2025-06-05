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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TableFunctionInvocation
        extends Relation
{
    private final QualifiedName name;
    private final List<TableFunctionArgument> arguments;
    private final List<List<QualifiedName>> copartitioning;

    public TableFunctionInvocation(NodeLocation location, QualifiedName name, List<TableFunctionArgument> arguments, List<List<QualifiedName>> copartitioning)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.copartitioning = requireNonNull(copartitioning, "copartitioning is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<TableFunctionArgument> getArguments()
    {
        return arguments;
    }

    public List<List<QualifiedName>> getCopartitioning()
    {
        return copartitioning;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunctionInvocation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return arguments;
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

        TableFunctionInvocation that = (TableFunctionInvocation) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(copartitioning, that.copartitioning);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments, copartitioning);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(name)
                .append("(");
        builder.append(arguments.stream()
                .map(TableFunctionArgument::toString)
                .collect(Collectors.joining(", ")));
        if (!copartitioning.isEmpty()) {
            builder.append(" COPARTITION");
            builder.append(copartitioning.stream()
                    .map(list -> list.stream()
                            .map(QualifiedName::toString)
                            .collect(Collectors.joining(", ", "(", ")")))
                    .collect(Collectors.joining(", ")));
        }
        builder.append(")");

        return builder.toString();
    }

    @Override
    public boolean shallowEquals(Node o)
    {
        if (!sameClass(this, o)) {
            return false;
        }

        TableFunctionInvocation other = (TableFunctionInvocation) o;
        return Objects.equals(name, other.name) &&
                Objects.equals(copartitioning, other.copartitioning);
    }
}
