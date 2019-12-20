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
package com.facebook.presto.benchmark.executor;

import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.BenchmarkRunnerConfig;
import com.facebook.presto.benchmark.framework.QueryException;
import com.facebook.presto.benchmark.prestoaction.PrestoAction;
import com.facebook.presto.benchmark.prestoaction.PrestoActionFactory;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.SUCCEEDED;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public class BenchmarkQueryExecutor
        implements QueryExecutor
{
    private static final Logger log = Logger.get(BenchmarkQueryExecutor.class);

    private final PrestoActionFactory prestoActionFactory;
    private final SqlParser sqlParser;
    private final ParsingOptions parsingOptions;
    private final Set<EventClient> eventClients;
    private final String testId;

    @Inject
    public BenchmarkQueryExecutor(
            PrestoActionFactory prestoActionFactory,
            SqlParser sqlParser,
            ParsingOptions parsingOptions,
            Set<EventClient> eventClients,
            BenchmarkRunnerConfig benchmarkRunnerConfig)
    {
        this.prestoActionFactory = requireNonNull(prestoActionFactory, "prestoAction is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
        this.testId = requireNonNull(benchmarkRunnerConfig, "testId is null").getTestId();
    }

    @Override
    public BenchmarkQueryEvent run(BenchmarkQuery benchmarkQuery, Map<String, String> sessionProperties)
    {
        QueryStats queryStats = null;
        Statement statement = sqlParser.createStatement(benchmarkQuery.getQuery(), parsingOptions);
        PrestoAction prestoAction = prestoActionFactory.get(benchmarkQuery, sessionProperties);

        try {
            queryStats = prestoAction.execute(statement);
            return postEvent(buildEvent(benchmarkQuery, Optional.ofNullable(queryStats), Optional.empty()));
        }
        catch (QueryException e) {
            return postEvent(buildEvent(benchmarkQuery, Optional.ofNullable(queryStats), Optional.of(e)));
        }
        catch (Throwable t) {
            log.error(t);
            return postEvent(buildEvent(benchmarkQuery, Optional.ofNullable(queryStats), Optional.empty()));
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
        String errorMessage = null;
        String stackTrace = null;

        if (!succeeded && queryException.isPresent()) {
            queryStats = queryException.get().getQueryStats();
            errorCode = queryException.get().getPrestoErrorCode().map(ErrorCodeSupplier::toErrorCode).map(ErrorCode::getName);
            errorMessage = queryException.get().getMessage();
            stackTrace = getStackTraceAsString(queryException.get().getCause());
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
                Optional.ofNullable(errorMessage),
                Optional.ofNullable(stackTrace));
    }

    private BenchmarkQueryEvent postEvent(BenchmarkQueryEvent event)
    {
        for (EventClient eventClient : eventClients) {
            eventClient.post(event);
        }
        return event;
    }
}
