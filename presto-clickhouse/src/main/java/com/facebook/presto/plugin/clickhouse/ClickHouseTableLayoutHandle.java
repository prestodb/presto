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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ClickHouseTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ClickHouseTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<ClickHouseExpression> additionalPredicate;
    private Optional<String> simpleExpression;

    @JsonCreator
    public ClickHouseTableLayoutHandle(
            @JsonProperty("table") ClickHouseTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain,
            @JsonProperty("additionalPredicate") Optional<ClickHouseExpression> additionalPredicate,
            @JsonProperty("simpleExpression") Optional<String> simpleExpression)
    {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
        this.additionalPredicate = additionalPredicate;
        this.simpleExpression = simpleExpression;
    }
    @JsonProperty
    public Optional<String> getSimpleExpression()
    {
        return simpleExpression;
    }

    @JsonProperty
    public Optional<ClickHouseExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public ClickHouseTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
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
        ClickHouseTableLayoutHandle that = (ClickHouseTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain) &&
                Objects.equals(additionalPredicate, that.additionalPredicate) &&
                Objects.equals(simpleExpression, that.simpleExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain, additionalPredicate, simpleExpression);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
