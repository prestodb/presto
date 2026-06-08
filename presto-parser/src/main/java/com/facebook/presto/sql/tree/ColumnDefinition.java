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

import com.facebook.presto.spi.derivedColumns.DerivedColumnSpec;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ColumnDefinition
        extends TableElement
{
    private final Identifier name;
    private final String type;
    private final boolean nullable;
    private final List<Property> properties;
    private final Optional<String> comment;
    private final Optional<Expression> defaultExpression;
    private Optional<Expression> derivedColumnExpression;
    private final Optional<DerivedColumnSpec> derivedColumnExpressionSpec;

    public ColumnDefinition(Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment)
    {
        this(Optional.empty(), name, type, nullable, properties, comment, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public ColumnDefinition(Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment, Optional<Expression> defaultExpression)
    {
        this(Optional.empty(), name, type, nullable, properties, comment, defaultExpression, Optional.empty(), Optional.empty());
    }

    public ColumnDefinition(NodeLocation location, Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment)
    {
        this(Optional.of(location), name, type, nullable, properties, comment, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public ColumnDefinition(NodeLocation location, Identifier name, String type, boolean nullable, List<Property> properties, Optional<String> comment, Optional<Expression> defaultExpression)
    {
        this(Optional.of(location), name, type, nullable, properties, comment, defaultExpression, Optional.empty(), Optional.empty());
    }

    public ColumnDefinition(
            Optional<NodeLocation> location,
            Identifier name,
            String type,
            boolean nullable,
            List<Property> properties,
            Optional<String> comment,
            Expression derivedColumnExpression,
            DerivedColumnSpec derivedColumnSpec)
    {
        this(location, name, type, nullable, properties, comment, Optional.empty(), Optional.ofNullable(derivedColumnExpression), Optional.of(derivedColumnSpec));
    }

    private ColumnDefinition(
            Optional<NodeLocation> location,
            Identifier name,
            String type,
            boolean nullable,
            List<Property> properties,
            Optional<String> comment,
            Optional<Expression> defaultExpression,
            Optional<Expression> derivedColumnExpression,
            Optional<DerivedColumnSpec> derivedColumnExpressionSpec)
    {
        super(location);
        checkArgument(!(defaultExpression.isPresent() && derivedColumnExpression.isPresent()),
                "Both 'default expression' and 'derived column definition' is currently not supported on same column.");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.nullable = nullable;
        this.properties = requireNonNull(properties, "properties is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.defaultExpression = requireNonNull(defaultExpression, "defaultExpression is null");
        this.derivedColumnExpression = requireNonNull(derivedColumnExpression, "derivedColumnExpression is null");
        this.derivedColumnExpressionSpec = requireNonNull(derivedColumnExpressionSpec, "derivedColumnExpressionSpec is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Optional<Expression> getDefaultExpression()
    {
        return defaultExpression;
    }

    public Optional<DerivedColumnSpec> getDerivedColumnExpressionSpec()
    {
        return derivedColumnExpressionSpec;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnDefinition(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        defaultExpression.ifPresent(children::add);
        derivedColumnExpression.ifPresent(children::add);
        return children.build();
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
        ColumnDefinition o = (ColumnDefinition) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.type, o.type) &&
                this.nullable == o.nullable &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(this.comment, o.comment) &&
                Objects.equals(this.defaultExpression, o.defaultExpression) &&
                Objects.equals(this.derivedColumnExpression, o.derivedColumnExpression) &&
                Objects.equals(this.derivedColumnExpressionSpec, o.derivedColumnExpressionSpec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, properties, comment, nullable, defaultExpression, derivedColumnExpression, derivedColumnExpressionSpec);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("nullable", nullable)
                .add("properties", properties)
                .add("comment", comment)
                .add("defaultExpression", defaultExpression)
                .add("derivedColumnExpression", derivedColumnExpression)
                .add("derivedColumnExpressionSpec", derivedColumnExpressionSpec)
                .toString();
    }
}
