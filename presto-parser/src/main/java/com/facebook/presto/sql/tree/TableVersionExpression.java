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

import static java.util.Objects.requireNonNull;

public class TableVersionExpression
        extends Expression
{
    public enum TableVersionType
    {
        TIMESTAMP,
        VERSION
    }

    private final Expression asOfExpression;
    private final TableVersionType type;

    public TableVersionExpression(TableVersionType type, Expression value)
    {
        this(Optional.empty(), type, value);
    }

    public TableVersionExpression(NodeLocation location, TableVersionType type, Expression value)
    {
        this(Optional.of(location), type, value);
    }

    private TableVersionExpression(Optional<NodeLocation> location, TableVersionType type, Expression value)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(type, "type is null");

        this.asOfExpression = value;
        this.type = type;
    }

    public static TableVersionExpression timestampExpression(NodeLocation location, Expression value)
    {
        return new TableVersionExpression(Optional.of(location), TableVersionType.TIMESTAMP, value);
    }

    public static TableVersionExpression versionExpression(NodeLocation location, Expression value)
    {
        return new TableVersionExpression(Optional.of(location), TableVersionType.VERSION, value);
    }

    public static TableVersionExpression timestampExpression(Expression value)
    {
        return new TableVersionExpression(Optional.empty(), TableVersionType.TIMESTAMP, value);
    }

    public static TableVersionExpression versionExpression(Expression value)
    {
        return new TableVersionExpression(Optional.empty(), TableVersionType.VERSION, value);
    }

    public Expression getAsOfExpression()
    {
        return asOfExpression;
    }

    public TableVersionType getTableVersionType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableVersion(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(asOfExpression);
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

        TableVersionExpression that = (TableVersionExpression) o;
        return Objects.equals(asOfExpression, that.asOfExpression) &&
                (type == that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(asOfExpression, type);
    }
}
