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

import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("QueryInfo")
public class QueryInfo
{
    private final String catalog;
    private final String schema;
    private final String originalQuery;
    private final String queryId;
    private final String checksumQueryId;
    private final String query;
    private final List<String> setupQueries;
    private final List<String> teardownQueries;
    private final String checksumQuery;
    private final Double cpuTimeSecs;
    private final Double wallTimeSecs;

    public QueryInfo(
            String catalog,
            String schema,
            String originalQuery,
            Optional<String> queryId,
            Optional<String> checksumQueryId,
            Optional<String> query,
            Optional<List<String>> setupQueries,
            Optional<List<String>> teardownQueries,
            Optional<String> checksumQuery,
            Optional<Double> cpuTimeSecs,
            Optional<Double> wallTimeSecs)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
        this.queryId = queryId.orElse(null);
        this.checksumQueryId = checksumQueryId.orElse(null);
        this.query = query.orElse(null);
        this.setupQueries = setupQueries.orElse(null);
        this.teardownQueries = teardownQueries.orElse(null);
        this.checksumQuery = checksumQuery.orElse(null);
        this.cpuTimeSecs = cpuTimeSecs.orElse(null);
        this.wallTimeSecs = wallTimeSecs.orElse(null);
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
}
