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
package com.facebook.presto.delta;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final DeltaTableHandle table;
    private final TupleDomain<DeltaColumnHandle> predicate;
    private final Optional<String> predicateText;

    @JsonCreator
    public DeltaTableLayoutHandle(
            @JsonProperty("table") DeltaTableHandle table,
            @JsonProperty("predicate") TupleDomain<DeltaColumnHandle> predicate,
            @JsonProperty("predicateText") Optional<String> predicateText)
    {
        this.table = table;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.predicateText = requireNonNull(predicateText, "predicateText is null");
    }

    @JsonProperty
    public DeltaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<DeltaColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public Optional<String> getPredicateText()
    {
        return predicateText;
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
        DeltaTableLayoutHandle that = (DeltaTableLayoutHandle) o;
        return Objects.equals(table, that.table) && Objects.equals(this.predicate, that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, predicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("predicate", predicateText)
                .toString();
    }
}
