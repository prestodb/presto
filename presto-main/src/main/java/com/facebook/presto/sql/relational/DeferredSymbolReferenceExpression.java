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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeferredSymbolReferenceExpression
        extends RowExpression
{
    private final String sourceId;
    private final String symbol;
    private final Type type;

    public DeferredSymbolReferenceExpression(String sourceId, String symbol, Type type)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.symbol = requireNonNull(symbol, "symbol is null");
        this.type = requireNonNull(type, "type is null");
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getSymbol()
    {
        return symbol;
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeferredSymbolReferenceExpression that = (DeferredSymbolReferenceExpression) o;
        return Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(symbol, that.symbol) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceId, symbol, type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceId", sourceId)
                .add("symbol", symbol)
                .add("type", type)
                .toString();
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeferredSymbolReference(this, context);
    }
}
