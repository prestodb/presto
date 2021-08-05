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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class JdbcSortItem
{
    private final JdbcColumnHandle column;
    private final SortOrder sortOrder;

    @JsonCreator
    public JdbcSortItem(JdbcColumnHandle column, SortOrder sortOrder)
    {
        this.column = requireNonNull(column, "column is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
    }

    @JsonProperty
    public JdbcColumnHandle getColumn()
    {
        return column;
    }

    @JsonProperty
    public SortOrder getSortOrder()
    {
        return sortOrder;
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
        JdbcSortItem that = (JdbcSortItem) o;
        return sortOrder == that.sortOrder &&
                Objects.equals(column, that.column);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, sortOrder);
    }

    @Override
    public String toString()
    {
        return column + " " + sortOrder;
    }
}
