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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Immutable
@EventType("QueryInfo")
public class QueryInfo
{
    private final String catalog;
    private final String schema;
    private final String originalQuery;
    private final String queryId;
    private final List<String> setupQueryIds;
    private final List<String> teardownQueryIds;
    private final String checksumQueryId;
    private final String query;
    private final List<String> setupQueries;
    private final List<String> teardownQueries;
    private final String checksumQuery;

    private final Double cpuTimeSecs;
    private final Double wallTimeSecs;
    private final Long peakTotalMemoryBytes;
    private final Long peakTaskTotalMemoryBytes;

    private final String extraStats;

    public QueryInfo(
            String catalog,
            String schema,
            String originalQuery)
    {
        this(
                catalog,
                schema,
                originalQuery,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public QueryInfo(
            String catalog,
            String schema,
            String originalQuery,
            List<String> setupQueryIds,
            List<String> teardownQueryIds,
            Optional<String> checksumQueryId,
            Optional<String> query,
            Optional<List<String>> setupQueries,
            Optional<List<String>> teardownQueries,
            Optional<String> checksumQuery,
            Optional<QueryActionStats> queryActionStats)
    {
        Optional<QueryStats> stats = queryActionStats.flatMap(QueryActionStats::getQueryStats);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
        this.queryId = stats.map(QueryStats::getQueryId).orElse(null);
        this.setupQueryIds = ImmutableList.copyOf(setupQueryIds);
        this.teardownQueryIds = ImmutableList.copyOf(teardownQueryIds);
        this.checksumQueryId = checksumQueryId.orElse(null);
        this.query = query.orElse(null);
        this.setupQueries = setupQueries.orElse(null);
        this.teardownQueries = teardownQueries.orElse(null);
        this.checksumQuery = checksumQuery.orElse(null);
        this.cpuTimeSecs = stats.map(QueryStats::getCpuTimeMillis).map(QueryInfo::millisToSeconds).orElse(null);
        this.wallTimeSecs = stats.map(QueryStats::getWallTimeMillis).map(QueryInfo::millisToSeconds).orElse(null);
        this.peakTotalMemoryBytes = stats.map(QueryStats::getPeakTotalMemoryBytes).orElse(null);
        this.peakTaskTotalMemoryBytes = stats.map(QueryStats::getPeakTaskTotalMemoryBytes).orElse(null);
        this.extraStats = queryActionStats.flatMap(QueryActionStats::getExtraStats).orElse(null);
    }

    private static double millisToSeconds(long millis)
    {
        return new Duration(millis, MILLISECONDS).getValue(SECONDS);
    }

    @EventField
    public String getCatalog()
    {
        return catalog;
    }

    @EventField
    public String getSchema()
    {
        return schema;
    }

    @EventField
    public String getOriginalQuery()
    {
        return originalQuery;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public List<String> getSetupQueryIds()
    {
        return setupQueryIds;
    }

    @EventField
    public List<String> getTeardownQueryIds()
    {
        return teardownQueryIds;
    }

    @EventField
    public String getChecksumQueryId()
    {
        return checksumQueryId;
    }

    @EventField
    public String getQuery()
    {
        return query;
    }

    @EventField
    public List<String> getSetupQueries()
    {
        return setupQueries;
    }

    @EventField
    public List<String> getTeardownQueries()
    {
        return teardownQueries;
    }

    @EventField
    public String getChecksumQuery()
    {
        return checksumQuery;
    }

    @EventField
    public Double getCpuTimeSecs()
    {
        return cpuTimeSecs;
    }

    @EventField
    public Double getWallTimeSecs()
    {
        return wallTimeSecs;
    }

    @EventField
    public Long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @EventField
    public Long getPeakTaskTotalMemoryBytes()
    {
        return peakTaskTotalMemoryBytes;
    }

    @EventField
    public String getExtraStats()
    {
        return extraStats;
    }
}
