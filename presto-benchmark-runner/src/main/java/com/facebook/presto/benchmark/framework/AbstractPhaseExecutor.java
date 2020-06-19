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
package com.facebook.presto.benchmark.framework;

import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.prestoaction.PrestoAction;
import com.facebook.presto.benchmark.prestoaction.PrestoActionFactory;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.SUCCEEDED;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public abstract class AbstractPhaseExecutor<T extends PhaseSpecification>
        implements PhaseExecutor<T>
{
    private static final Logger log = Logger.get(AbstractPhaseExecutor.class);

    private final SqlParser sqlParser;
    private final ParsingOptions parsingOptions;
    private final PrestoActionFactory prestoActionFactory;
    private final Set<EventClient> eventClients;
    private final String testId;

    protected AbstractPhaseExecutor(
            SqlParser sqlParser,
            ParsingOptions parsingOptions,
            PrestoActionFactory prestoActionFactory,
            Set<EventClient> eventClients,
            String testId)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
        this.prestoActionFactory = requireNonNull(prestoActionFactory, "prestoActionFactory is null");
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
        this.testId = requireNonNull(testId, "testId is null");
    }

    protected BenchmarkQueryEvent runQuery(BenchmarkQuery benchmarkQuery)
    {
        Optional<QueryStats> queryStats = Optional.empty();
        Statement statement = sqlParser.createStatement(benchmarkQuery.getQuery(), parsingOptions);
        PrestoAction prestoAction = prestoActionFactory.get(benchmarkQuery);

        try {
            queryStats = Optional.of(prestoAction.execute(statement));
            return buildEvent(benchmarkQuery, queryStats, Optional.empty());
        }
        catch (QueryException e) {
            return buildEvent(benchmarkQuery, queryStats, Optional.of(e));
        }
        catch (Throwable t) {
            log.error(t);
            return buildEvent(benchmarkQuery, queryStats, Optional.empty());
        }
    }

    private BenchmarkQueryEvent buildEvent(
            BenchmarkQuery benchmarkQuery,
            Optional<QueryStats> queryStats,
            Optional<QueryException> queryException)
    {
        boolean succeeded = queryStats.isPresent();
        Status status = succeeded ? SUCCEEDED : FAILED;
        Optional<String> errorCode = Optional.empty();
        Optional<String> errorMessage = Optional.empty();
        Optional<String> stackTrace = Optional.empty();

        if (!succeeded && queryException.isPresent()) {
            queryStats = queryException.get().getQueryStats();
            errorCode = queryException.get().getPrestoErrorCode().map(ErrorCodeSupplier::toErrorCode).map(ErrorCode::getName);
            errorMessage = Optional.of(queryException.get().getMessage());
            stackTrace = Optional.of(getStackTraceAsString(queryException.get().getCause()));
        }

        return new BenchmarkQueryEvent(
                testId,
                benchmarkQuery.getName(),
                status,
                benchmarkQuery.getCatalog(),
                benchmarkQuery.getSchema(),
                benchmarkQuery.getQuery(),
                queryStats,
                errorCode,
                errorMessage,
                stackTrace);
    }

    protected void postEvent(BenchmarkQueryEvent event)
    {
        for (EventClient eventClient : eventClients) {
            eventClient.post(event);
        }
    }

    protected static BenchmarkQuery overrideSessionProperties(BenchmarkQuery query, Map<String, String> override)
    {
        Map<String, String> sessionProperties = new HashMap<>(query.getSessionProperties());
        for (Entry<String, String> entry : override.entrySet()) {
            sessionProperties.put(entry.getKey(), entry.getValue());
        }
        return new BenchmarkQuery(
                query.getName(),
                query.getQuery(),
                query.getCatalog(),
                query.getSchema(),
                Optional.of(sessionProperties));
    }
}
