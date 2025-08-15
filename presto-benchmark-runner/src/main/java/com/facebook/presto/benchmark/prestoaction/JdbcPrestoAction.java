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
package com.facebook.presto.benchmark.prestoaction;

import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.QueryException;
import com.facebook.presto.benchmark.framework.QueryResult;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.facebook.presto.benchmark.retry.RetryDriver;
import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.benchmark.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcPrestoAction
        implements PrestoAction
{
    private static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";

    private final SqlExceptionClassifier exceptionClassifier;
    private final BenchmarkQuery benchmarkQuery;
    private final String jdbcUrl;
    private final Duration queryTimeout;
    private final RetryDriver networkRetry;

    public JdbcPrestoAction(
            SqlExceptionClassifier exceptionClassifier,
            BenchmarkQuery benchmarkQuery,
            PrestoClusterConfig prestoClusterConfig,
            RetryConfig networkRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.benchmarkQuery = requireNonNull(benchmarkQuery, "benchmarkQuery is null");
        this.jdbcUrl = requireNonNull(prestoClusterConfig.getJdbcUrl(), "jdbcUrl is null");
        this.queryTimeout = requireNonNull(prestoClusterConfig.getQueryTimeout(), "queryTimeout is null");

        this.networkRetry = new RetryDriver(
                networkRetryConfig,
                (exception) -> exception instanceof QueryException
                        && (((QueryException) exception).getType() == CLUSTER_CONNECTION));
    }

    @Override
    public QueryStats execute(Statement statement)
    {
        return execute(statement, Optional.empty()).getQueryStats();
    }

    @Override
    public <R> QueryResult<R> execute(Statement statement, ResultSetConverter<R> converter)
    {
        return execute(statement, Optional.of(converter));
    }

    private <R> QueryResult<R> execute(Statement statement, Optional<ResultSetConverter<R>> converter)
    {
        return networkRetry.run(
                "presto-cluster-connection",
                () -> executeOnce(statement, converter));
    }

    private <R> QueryResult<R> executeOnce(Statement statement, Optional<ResultSetConverter<R>> converter)
    {
        String query = formatSql(statement, Optional.empty());
        ProgressMonitor progressMonitor = new ProgressMonitor();

        try (PrestoConnection connection = getConnection()) {
            try (java.sql.Statement jdbcStatement = connection.createStatement()) {
                PrestoStatement prestoStatement = jdbcStatement.unwrap(PrestoStatement.class);
                prestoStatement.setProgressMonitor(progressMonitor);

                ImmutableList.Builder<R> rows = ImmutableList.builder();
                if (converter.isPresent()) {
                    try (ResultSet resultSet = jdbcStatement.executeQuery(query)) {
                        while (resultSet.next()) {
                            rows.add(converter.get().apply(resultSet));
                        }
                    }
                }
                else {
                    boolean moreResults = jdbcStatement.execute(query);
                    if (moreResults) {
                        consumeResultSet(jdbcStatement.getResultSet());
                    }
                    do {
                        moreResults = jdbcStatement.getMoreResults();
                        if (moreResults) {
                            consumeResultSet(jdbcStatement.getResultSet());
                        }
                    }
                    while (moreResults || jdbcStatement.getUpdateCount() != -1);
                }

                checkState(progressMonitor.getLastQueryStats().isPresent(), "lastQueryStats is missing");
                return new QueryResult<>(rows.build(), progressMonitor.getLastQueryStats().get());
            }
        }
        catch (SQLException e) {
            throw exceptionClassifier.createException(progressMonitor.getLastQueryStats(), e);
        }
    }

    private void consumeResultSet(ResultSet resultSet)
            throws SQLException
    {
        while (resultSet.next()) {
            // Do nothing
        }
    }

    private PrestoConnection getConnection()
            throws SQLException
    {
        PrestoConnection connection = DriverManager.getConnection(jdbcUrl, "user", null)
                .unwrap(PrestoConnection.class);

        try {
            connection.setClientInfo("ApplicationName", "benchmark-test");
            connection.setCatalog(benchmarkQuery.getCatalog());
            connection.setSchema(benchmarkQuery.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }

        Map<String, String> sessionProperties = ImmutableMap.<String, String>builder()
                .putAll(benchmarkQuery.getSessionProperties())
                .put(QUERY_MAX_EXECUTION_TIME, queryTimeout.toString())
                .build();
        for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
            connection.setSessionProperty(entry.getKey(), entry.getValue());
        }
        return connection;
    }

    static class ProgressMonitor
            implements Consumer<QueryStats>
    {
        private Optional<QueryStats> queryStats = Optional.empty();

        @Override
        public synchronized void accept(QueryStats queryStats)
        {
            this.queryStats = Optional.of(requireNonNull(queryStats, "queryStats is null"));
        }

        public synchronized Optional<QueryStats> getLastQueryStats()
        {
            return queryStats;
        }
    }
}
