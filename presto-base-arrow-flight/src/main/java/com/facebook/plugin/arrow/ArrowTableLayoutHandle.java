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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrowTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final ArrowTableHandle tableHandle;
    private final List<ArrowColumnHandle> columnHandles;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<ArrowExpression> additionalPredicate;

    @JsonCreator
    public ArrowTableLayoutHandle(@JsonProperty("table") ArrowTableHandle table,
                                  @JsonProperty("columnHandles") List<ArrowColumnHandle> columnHandles,
                                  @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain,
                                  @JsonProperty("additionalPredicate") Optional<ArrowExpression> additionalPredicate)
    {
        this.tableHandle = requireNonNull(table, "table is null");
        this.columnHandles = requireNonNull(columnHandles, "columns are null");
        this.tupleDomain = requireNonNull(domain, "domain is null");
        this.additionalPredicate = additionalPredicate;
    }

    @JsonProperty("table")
    public ArrowTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty("tupleDomain")
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty("additionalPredicate")
    public Optional<ArrowExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty("columnHandles")
    public List<ArrowColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
}
