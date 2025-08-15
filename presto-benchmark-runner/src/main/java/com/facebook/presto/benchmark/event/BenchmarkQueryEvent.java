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
package com.facebook.presto.benchmark.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.presto.jdbc.QueryStats;
import io.airlift.units.Duration;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Immutable
@EventType("BenchmarkQuery")
public class BenchmarkQueryEvent
{
    public enum Status
    {
        SUCCEEDED,
        FAILED
    }

    private final String testId;
    private final String name;
    private final String status;
    private final String catalog;
    private final String schema;
    private final String query;
    private final String queryId;
    private final Double cpuTimeSecs;
    private final Double wallTimeSecs;
    private final String errorCode;
    private final String errorMessage;
    private final String stackTrace;

    public BenchmarkQueryEvent(
            String testId,
            String name,
            Status status,
            String catalog,
            String schema,
            String query,
            Optional<QueryStats> queryStats,
            Optional<String> errorCode,
            Optional<String> errorMessage,
            Optional<String> stackTrace)
    {
        this.testId = requireNonNull(testId, "testId is null");
        this.name = requireNonNull(name, "name is null");
        this.status = requireNonNull(status, "status is null").name();
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.query = requireNonNull(query, "query is null");
        this.queryId = requireNonNull(queryStats, "queryStats is null").map(QueryStats::getQueryId).orElse(null);
        this.cpuTimeSecs = millisToSeconds(queryStats.map(QueryStats::getCpuTimeMillis)).orElse(null);
        this.wallTimeSecs = millisToSeconds(queryStats.map(QueryStats::getWallTimeMillis)).orElse(null);
        this.errorCode = requireNonNull(errorCode, "errorCode is null").orElse(null);
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null").orElse(null);
        this.stackTrace = requireNonNull(stackTrace, "stackTrace is null").orElse(null);
    }

    @EventField
    public String getTestId()
    {
        return testId;
    }

    @EventField
    public String getName()
    {
        return name;
    }

    @EventField
    public String getStatus()
    {
        return status;
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
    public String getQuery()
    {
        return query;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public Double getCpuTime()
    {
        return cpuTimeSecs;
    }

    @EventField
    public Double getWallTime()
    {
        return wallTimeSecs;
    }

    @EventField
    public String getErrorCode()
    {
        return errorCode;
    }

    @EventField
    public String getErrorMessage()
    {
        return errorMessage;
    }

    @EventField
    public String getStackTrace()
    {
        return stackTrace;
    }

    public Status getEventStatus()
    {
        return Status.valueOf(status);
    }

    private static Optional<Double> millisToSeconds(Optional<Long> millis)
    {
        return millis.map(value -> new Duration(value, MILLISECONDS).getValue(SECONDS));
    }
}
