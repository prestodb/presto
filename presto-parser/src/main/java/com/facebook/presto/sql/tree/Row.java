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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class Row
        extends Expression
{
    private final List<Field> fields;

    public Row(List<Field> fields)
    {
        this(Optional.empty(), fields);
    }

    public Row(NodeLocation location, List<Field> fields)
    {
        this(Optional.of(location), fields);
    }

    private Row(Optional<NodeLocation> location, List<Field> fields)
    {
        super(location);
        requireNonNull(fields, "fields is null");
        this.fields = ImmutableList.copyOf(fields);
    }

    /**
     * Creates a row where no field declares a name, e.g. {@code ROW(1, 2)}.
     */
    public static Row unnamed(List<Expression> items)
    {
        return new Row(toUnnamedFields(items));
    }

    /**
     * Creates a row where no field declares a name, e.g. {@code ROW(1, 2)}.
     */
    public static Row unnamed(NodeLocation location, List<Expression> items)
    {
        return new Row(location, toUnnamedFields(items));
    }

    private static List<Field> toUnnamedFields(List<Expression> items)
    {
        requireNonNull(items, "items is null");
        return items.stream()
                .map(Field::new)
                .collect(toImmutableList());
    }

    public List<Field> getFields()
    {
        return fields;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return fields;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
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
        Row other = (Row) obj;
        return Objects.equals(this.fields, other.fields);
    }

    /**
     * A single field of a row constructor: an expression with an optionally declared field name,
     * e.g. the {@code 1 AS a} in {@code ROW(1 AS a, 2)}.
     */
    public static final class Field
            extends Node
    {
        private final Optional<Identifier> name;
        private final Expression expression;

        /**
         * The field takes the location of its expression. {@link Row#getChildren()} returns fields
         * rather than the expressions themselves, so without this a field would hide the only
         * location available to helpers that recover a location from a node's children, such as
         * {@code ExpressionTreeUtils#getSourceLocation}.
         */
        public Field(Expression expression)
        {
            this(locationOf(expression), Optional.empty(), expression);
        }

        public Field(Optional<Identifier> name, Expression expression)
        {
            this(locationOf(expression), name, expression);
        }

        public Field(NodeLocation location, Optional<Identifier> name, Expression expression)
        {
            this(Optional.of(location), name, expression);
        }

        public Field(Optional<NodeLocation> location, Optional<Identifier> name, Expression expression)
        {
            super(location);
            this.name = requireNonNull(name, "name is null");
            this.expression = requireNonNull(expression, "expression is null");
        }

        private static Optional<NodeLocation> locationOf(Expression expression)
        {
            return requireNonNull(expression, "expression is null").getLocation();
        }

        public Optional<Identifier> getName()
        {
            return name;
        }

        public Expression getExpression()
        {
            return expression;
        }

        @Override
        protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
        {
            return visitor.visitRowField(this, context);
        }

        @Override
        public List<? extends Node> getChildren()
        {
            ImmutableList.Builder<Node> children = ImmutableList.builder();
            name.ifPresent(children::add);
            children.add(expression);
            return children.build();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, expression);
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
            Field other = (Field) obj;
            return Objects.equals(this.name, other.name) &&
                    Objects.equals(this.expression, other.expression);
        }

        @Override
        public String toString()
        {
            if (name.isPresent()) {
                return expression + " AS " + name.get();
            }
            return expression.toString();
        }
    }
}
