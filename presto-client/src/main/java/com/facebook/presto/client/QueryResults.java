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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.client.FixJsonDataUtils.fixData;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryResults
        implements QueryStatusInfo, QueryData
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;
    private final StatementStats stats;
    private final QueryError error;
    private final String updateType;
    private final Long updateCount;

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this(id, infoUri, partialCancelUri, nextUri, columns, fixData(columns, data), stats, error, updateType, updateCount);
    }

    public QueryResults(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Column> columns,
            Iterable<List<Object>> data,
            StatementStats stats,
            QueryError error,
            String updateType,
            Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
        checkArgument(data == null || columns != null, "data present without columns");
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @Nonnull
    @JsonProperty
    @Override
    public String getId()
    {
        return id;
    }

    @Nonnull
    @JsonProperty
    @Override
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    @Override
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Nonnull
    @JsonProperty
    @Override
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    @Override
    public QueryError getError()
    {
        return error;
    }

    @Nullable
    @JsonProperty
    @Override
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    @Override
    public Long getUpdateCount()
    {
        return updateCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("hasData", data != null)
                .add("stats", stats)
                .add("error", error)
                .add("updateType", updateType)
                .add("updateCount", updateCount)
                .toString();
    }
}
