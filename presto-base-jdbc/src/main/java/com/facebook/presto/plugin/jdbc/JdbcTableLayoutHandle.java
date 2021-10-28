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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JdbcTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final JdbcTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<JdbcExpression> additionalPredicate;
    private final String layoutString;

    public JdbcTableLayoutHandle(
            SqlFunctionProperties properties,
            JdbcTableHandle table,
            TupleDomain<ColumnHandle> domain,
            Optional<JdbcExpression> additionalPredicate)
    {
        this(table,
                domain,
                additionalPredicate,
                toStringHelper("")
                        .add("domains", domain.transform((columnHandle) -> ((JdbcColumnHandle) columnHandle).getColumnName()).toString(properties))
                        .add("additionalPredicate", additionalPredicate.map(JdbcExpression::getExpression).orElse("{}"))
                        .toString());
    }

    @JsonCreator
    public JdbcTableLayoutHandle(
            @JsonProperty("table") JdbcTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain,
            @JsonProperty("additionalPredicate") Optional<JdbcExpression> additionalPredicate,
            @JsonProperty("layoutString") String layoutString)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
        this.additionalPredicate = additionalPredicate;
        this.layoutString = requireNonNull(layoutString, "layoutString is null");
    }

    @JsonProperty
    public Optional<JdbcExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public JdbcTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public String getLayoutString()
    {
        return layoutString;
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
        JdbcTableLayoutHandle that = (JdbcTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain) &&
                Objects.equals(additionalPredicate, that.additionalPredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain, additionalPredicate);
    }

    @Override
    public String toString()
    {
        return layoutString;
    }
}
