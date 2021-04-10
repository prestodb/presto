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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
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
    private final Map<String, String> sessionProperties;
    private final String queryId;
    private final List<String> setupQueryIds;
    private final List<String> teardownQueryIds;
    private final String checksumQueryId;
    private final String query;
    private final List<String> setupQueries;
    private final List<String> teardownQueries;
    private final String checksumQuery;
    private final String jsonPlan;
    private final String outputTableName;

    private final Double cpuTimeSecs;
    private final Double wallTimeSecs;
    private final Long peakTotalMemoryBytes;
    private final Long peakTaskTotalMemoryBytes;

    private final String extraStats;

    public QueryInfo(
            String catalog,
            String schema,
            String originalQuery,
            Map<String, String> sessionProperties,
            List<String> setupQueryIds,
            List<String> teardownQueryIds,
            Optional<String> checksumQueryId,
            Optional<String> query,
            Optional<List<String>> setupQueries,
            Optional<List<String>> teardownQueries,
            Optional<String> checksumQuery,
            Optional<String> jsonPlan,
            Optional<QueryActionStats> queryActionStats,
            Optional<String> outputTableName)
    {
        Optional<QueryStats> stats = queryActionStats.flatMap(QueryActionStats::getQueryStats);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
        this.queryId = stats.map(QueryStats::getQueryId).orElse(null);
        this.setupQueryIds = ImmutableList.copyOf(setupQueryIds);
        this.teardownQueryIds = ImmutableList.copyOf(teardownQueryIds);
        this.checksumQueryId = checksumQueryId.orElse(null);
        this.query = query.orElse(null);
        this.setupQueries = setupQueries.orElse(null);
        this.teardownQueries = teardownQueries.orElse(null);
        this.checksumQuery = checksumQuery.orElse(null);
        this.jsonPlan = jsonPlan.orElse(null);
        this.cpuTimeSecs = stats.map(QueryStats::getCpuTimeMillis).map(QueryInfo::millisToSeconds).orElse(null);
        this.wallTimeSecs = stats.map(QueryStats::getWallTimeMillis).map(QueryInfo::millisToSeconds).orElse(null);
        this.peakTotalMemoryBytes = stats.map(QueryStats::getPeakTotalMemoryBytes).orElse(null);
        this.peakTaskTotalMemoryBytes = stats.map(QueryStats::getPeakTaskTotalMemoryBytes).orElse(null);
        this.extraStats = queryActionStats.flatMap(QueryActionStats::getExtraStats).orElse(null);
        this.outputTableName = outputTableName.orElse(null);
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
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
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
    public String getJsonPlan()
    {
        return jsonPlan;
    }

    @EventField
    public String getOutputTableName()
    {
        return outputTableName;
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

    public static Builder builder(
            String catalog,
            String schema,
            String originalQuery,
            Map<String, String> sessionProperties)
    {
        return new Builder(catalog, schema, originalQuery, sessionProperties);
    }

    public static class Builder
    {
        private final String catalog;
        private final String schema;
        private final String originalQuery;
        private final Map<String, String> sessionProperties;
        private List<String> setupQueryIds = ImmutableList.of();
        private List<String> teardownQueryIds = ImmutableList.of();
        private Optional<String> checksumQueryId = Optional.empty();
        private Optional<String> query = Optional.empty();
        private Optional<List<String>> setupQueries = Optional.empty();
        private Optional<List<String>> teardownQueries = Optional.empty();
        private Optional<String> checksumQuery = Optional.empty();
        private Optional<String> jsonPlan = Optional.empty();
        private Optional<QueryActionStats> queryActionStats = Optional.empty();
        private Optional<String> outputTableName = Optional.empty();

        private Builder(
                String catalog,
                String schema,
                String originalQuery,
                Map<String, String> sessionProperties)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
            this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
            this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
        }

        public Builder setSetupQueryIds(List<String> setupQueryIds)
        {
            this.setupQueryIds = ImmutableList.copyOf(setupQueryIds);
            return this;
        }

        public Builder setTeardownQueryIds(List<String> teardownQueryIds)
        {
            this.teardownQueryIds = ImmutableList.copyOf(teardownQueryIds);
            return this;
        }

        public Builder setChecksumQueryId(Optional<String> checksumQueryId)
        {
            this.checksumQueryId = requireNonNull(checksumQueryId, "checksumQueryId is null");
            return this;
        }

        public Builder setQuery(Optional<String> query)
        {
            this.query = requireNonNull(query, "query is null");
            return this;
        }

        public Builder setSetupQueries(Optional<List<String>> setupQueries)
        {
            this.setupQueries = requireNonNull(setupQueries, "setupQueries is null");
            return this;
        }

        public Builder setTeardownQueries(Optional<List<String>> teardownQueries)
        {
            this.teardownQueries = requireNonNull(teardownQueries, "teardownQueries is null");
            return this;
        }

        public Builder setChecksumQuery(Optional<String> checksumQuery)
        {
            this.checksumQuery = requireNonNull(checksumQuery, "checksumQuery is null");
            return this;
        }

        public Builder setJsonPlan(String jsonPlan)
        {
            this.jsonPlan = Optional.of(jsonPlan);
            return this;
        }

        public Builder setQueryActionStats(Optional<QueryActionStats> queryActionStats)
        {
            this.queryActionStats = requireNonNull(queryActionStats, "queryActionStats is null");
            return this;
        }

        public Builder setOutputTableName(Optional<String> outputTableName)
        {
            this.outputTableName = requireNonNull(outputTableName, "outputTableName is null");
            return this;
        }

        public QueryInfo build()
        {
            return new QueryInfo(
                    catalog,
                    schema,
                    originalQuery,
                    sessionProperties,
                    setupQueryIds,
                    teardownQueryIds,
                    checksumQueryId,
                    query,
                    setupQueries,
                    teardownQueries,
                    checksumQuery,
                    jsonPlan,
                    queryActionStats,
                    outputTableName);
        }
    }
}
