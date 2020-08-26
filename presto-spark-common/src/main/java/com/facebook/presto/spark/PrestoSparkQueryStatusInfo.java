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
package com.facebook.presto.spark;

import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.spi.PrestoWarning;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Based on the com.facebook.presto.client.QueryResults
 * <p>
 * The PrestoSparkQueryStatusInfo does not contain data to keep it slim.
 * Presto on Spark stores query results in a separate file.
 */
@Immutable
public class PrestoSparkQueryStatusInfo
{
    private final String id;
    private final Optional<List<Column>> columns;
    private final StatementStats stats;
    private final Optional<QueryError> error;
    private final List<PrestoWarning> warnings;
    private final Optional<String> updateType;
    private final OptionalLong updateCount;

    @JsonCreator
    public PrestoSparkQueryStatusInfo(
            @JsonProperty("id") String id,
            @JsonProperty("columns") Optional<List<Column>> columns,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") Optional<QueryError> error,
            @JsonProperty("warnings") List<PrestoWarning> warnings,
            @JsonProperty("updateType") Optional<String> updateType,
            @JsonProperty("updateCount") OptionalLong updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.columns = requireNonNull(columns, "columns is null").map(ImmutableList::copyOf);
        this.stats = requireNonNull(stats, "stats is null");
        this.error = requireNonNull(error, "error is null");
        this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.updateCount = requireNonNull(updateCount, "updateCount is null");
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public Optional<List<Column>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public Optional<QueryError> getError()
    {
        return error;
    }

    @JsonProperty
    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    @JsonProperty
    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public OptionalLong getUpdateCount()
    {
        return updateCount;
    }
}
