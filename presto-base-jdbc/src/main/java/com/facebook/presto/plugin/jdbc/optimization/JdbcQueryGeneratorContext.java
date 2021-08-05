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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcQueryGeneratorContext
{
    private Optional<JdbcExpression> additionalPredicate;
    private Optional<List<JdbcSortItem>> sortOrder;
    private OptionalLong limit;

    public JdbcQueryGeneratorContext()
    {
        this(Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    @JsonCreator
    public JdbcQueryGeneratorContext(
            @JsonProperty("additionalPredicate") Optional<JdbcExpression> additionalPredicate,
            @JsonProperty("sortOrder") Optional<List<JdbcSortItem>> sortOrder,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.additionalPredicate = requireNonNull(additionalPredicate, "additionalPredicate is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    JdbcQueryGeneratorContext withLimit(OptionalLong limit)
    {
        checkState(!hasLimit(), "Limit already exists. Jdbc datasources doesn't support limit on top of another limit");
        this.limit = limit;
        return this;
    }

    JdbcQueryGeneratorContext withTopN(Optional<List<JdbcSortItem>> sortOrder, OptionalLong limit)
    {
        checkState(!hasLimit(), "Limit already exists. Jdbc datasources doesn't support limit on top of another limit");
        this.sortOrder = sortOrder;
        this.limit = limit;
        return this;
    }

    JdbcQueryGeneratorContext withFilter(Optional<JdbcExpression> additionalPredicate)
    {
        checkState(!hasFilter(), "The filter has been set!");
        this.additionalPredicate = additionalPredicate;
        return this;
    }

    @JsonProperty
    public Optional<JdbcExpression> getAdditionalPredicate()
    {
        return additionalPredicate;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<List<JdbcSortItem>> getSortOrder()
    {
        return sortOrder;
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasFilter()
    {
        return additionalPredicate.isPresent();
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

        JdbcQueryGeneratorContext context = (JdbcQueryGeneratorContext) o;
        return Objects.equals(additionalPredicate, context.additionalPredicate) &&
                Objects.equals(sortOrder, context.sortOrder) &&
                Objects.equals(limit, context.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(additionalPredicate, sortOrder, limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("additionalPredicate", additionalPredicate)
                .add("sortOrder", sortOrder)
                .add("limit", limit)
                .toString();
    }
}
