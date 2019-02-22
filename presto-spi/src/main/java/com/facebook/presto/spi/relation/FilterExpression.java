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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.relation.column.ColumnExpression;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class FilterExpression
        extends UnaryTableExpression
{
    private final ColumnExpression predicate;
    private final TableExpression source;

    public FilterExpression(ColumnExpression predicate, TableExpression source)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.source = requireNonNull(source, "source is null");
    }

    public ColumnExpression getPredicate()
    {
        return predicate;
    }

    @Override
    public List<ColumnExpression> getOutput()
    {
        return source.getOutput();
    }

    @Override
    public TableExpression getSource()
    {
        return source;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FilterExpression)) {
            return false;
        }
        FilterExpression that = (FilterExpression) o;
        return Objects.equals(predicate, that.predicate) &&
                Objects.equals(source, that.source);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicate, source);
    }

    @Override
    public String toString()
    {
        return "FilterExpression{" +
                "predicate=" + predicate +
                ", source=" + source +
                '}';
    }
}
