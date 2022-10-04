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

import com.facebook.presto.spi.PrestoWarning;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.client.FixJsonDataUtils.fixData;
import static com.google.common.base.MoreObjects.firstNonNull;
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
    private final List<PrestoWarning> warnings;
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
            @JsonProperty("warnings") List<PrestoWarning> warnings,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this(
                id,
                infoUri,
                partialCancelUri,
                nextUri,
                columns,
                fixData(columns, data),
                stats,
                error,
                firstNonNull(warnings, ImmutableList.of()),
                updateType,
                updateCount);
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
            List<PrestoWarning> warnings,
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
        this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    /**
     * Returns identifier of query that produces this result set
     */
    @JsonProperty
    @Override
    public String getId()
    {
        return id;
    }

    /**
     * Returns the URI at the coordinator that provides information about the query
     * @return {@link java.net.URI}
     */
    @JsonProperty
    @Override
    public URI getInfoUri()
    {
        return infoUri;
    }

    /**
     *  Returns URI to a leaf stage that is currently executing in order to issue a cancel signal
     * @return {@link java.net.URI}
     */
    @Nullable
    @JsonProperty
    @Override
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    /**
     *  Returns URI to get next batch of query results
     * @return {@link java.net.URI}
     */
    @Nullable
    @JsonProperty
    @Override
    public URI getNextUri()
    {
        return nextUri;
    }

    /**
     * Returns list of columns (with corresponding data types) present in the result set
     */
    @Nullable
    @JsonProperty
    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    /**
     * Returns an iterator to the payload (results)
     */
    @Nullable
    @JsonProperty
    @Override
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    /**
     * Returns cumulative statistics on the query being executed
     * @return {@link com.facebook.presto.client.StatementStats}
     */
    @JsonProperty
    @Override
    public StatementStats getStats()
    {
        return stats;
    }

    /**
     * Returns error encountered in query execution to the client
     * The client is expected to check this field before consuming results
     * @return {@link com.facebook.presto.client.QueryError}
     */
    @Nullable
    @JsonProperty
    @Override
    public QueryError getError()
    {
        return error;
    }

    /**
     * Returns a list of warnings encountered in query execution to the client
     * @return {@link com.facebook.presto.spi.PrestoWarning}
     */
    @JsonProperty
    @Override
    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    /**
     * Returns the update type, if any, of the query as determined by the Analyzer
     * Could be INSERT, DELETE, CREATE, etc.
     */
    @Nullable
    @JsonProperty
    @Override
    public String getUpdateType()
    {
        return updateType;
    }

    /**
     * Returns a count of number of rows updated by the query
     */
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
