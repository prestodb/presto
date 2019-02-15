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
package com.facebook.presto.spi.relation.column;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Objects;

public class ColumnReferenceExpression
        extends ColumnExpression
{
    private final ColumnHandle columnHandle;
    private final Type type;

    public ColumnReferenceExpression(ColumnHandle columnHandle, Type type)
    {
        this.columnHandle = columnHandle;
        this.type = type;
    }

    public ColumnHandle getColumnHandle()
    {
        return columnHandle;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnReferenceExpression)) {
            return false;
        }
        ColumnReferenceExpression that = (ColumnReferenceExpression) o;
        return Objects.equals(columnHandle, that.columnHandle) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnHandle, type);
    }

    @Override
    public String toString()
    {
        return "ColumnReferenceExpression{" +
                "columnHandle=" + columnHandle +
                ", type=" + type +
                '}';
    }

    @Override
    public ColumnExpression replaceChildren(List<ColumnExpression> newChildren)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(ColumnExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitColumnReference(this, context);
    }
}
