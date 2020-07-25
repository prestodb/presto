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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.verifier.framework.ClusterConnectionException;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryResult;
import com.facebook.presto.verifier.framework.QueryStage;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.ForPresto;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.retry.RetryDriver;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.verifier.framework.QueryStage.DETERMINISM_ANALYSIS_MAIN;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcPrestoAction
        implements PrestoAction
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final QueryConfiguration queryConfiguration;

    private final String jdbcUrl;
    private final Duration queryTimeout;
    private final Duration metadataTimeout;
    private final Duration checksumTimeout;

    private final RetryDriver<QueryException> networkRetry;
    private final RetryDriver<QueryException> prestoRetry;

    public JdbcPrestoAction(
            SqlExceptionClassifier exceptionClassifier,
            QueryConfiguration queryConfiguration,
            VerificationContext verificationContext,
            PrestoClusterConfig prestoClusterConfig,
            @ForClusterConnection RetryConfig networkRetryConfig,
            @ForPresto RetryConfig prestoRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.queryConfiguration = requireNonNull(queryConfiguration, "queryConfiguration is null");

        this.jdbcUrl = requireNonNull(prestoClusterConfig.getJdbcUrl(), "jdbcUrl is null");
        this.queryTimeout = requireNonNull(prestoClusterConfig.getQueryTimeout(), "queryTimeout is null");
        this.metadataTimeout = requireNonNull(prestoClusterConfig.getMetadataTimeout(), "metadataTimeout is null");
        this.checksumTimeout = requireNonNull(prestoClusterConfig.getChecksumTimeout(), "checksumTimeout is null");

        this.networkRetry = new RetryDriver<>(
                networkRetryConfig,
                queryException -> queryException instanceof ClusterConnectionException && queryException.isRetryable(),
                QueryException.class,
                verificationContext::addException);
        this.prestoRetry = new RetryDriver<>(
                prestoRetryConfig,
                queryException -> queryException instanceof PrestoQueryException && queryException.isRetryable(),
                QueryException.class,
                verificationContext::addException);
    }

    @Override
    public QueryStats execute(Statement statement, QueryStage queryStage)
    {
        return execute(statement, queryStage, new NoResultStatementExecutor<>());
    }

    @Override
    public <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter)
    {
        return execute(statement, queryStage, new ResultConvertingStatementExecutor<>(converter));
    }

    private <T> T execute(Statement statement, QueryStage queryStage, StatementExecutor<T> statementExecutor)
    {
        return prestoRetry.run(
                "presto",
                () -> networkRetry.run(
                        "presto-cluster-connection",
                        () -> executeOnce(statement, queryStage, statementExecutor)));
    }

    private <T> T executeOnce(Statement statement, QueryStage queryStage, StatementExecutor<T> statementExecutor)
    {
        String query = formatSql(statement, Optional.empty());

        try (PrestoConnection connection = getConnection(queryStage)) {
            try (java.sql.Statement jdbcStatement = connection.createStatement()) {
                PrestoStatement prestoStatement = jdbcStatement.unwrap(PrestoStatement.class);
                prestoStatement.setProgressMonitor(statementExecutor.getProgressMonitor());
                return statementExecutor.execute(prestoStatement, query);
            }
        }
        catch (SQLException e) {
            throw exceptionClassifier.createException(queryStage, statementExecutor.getProgressMonitor().getLastQueryStats(), e);
        }
    }

    private PrestoConnection getConnection(QueryStage queryStage)
            throws SQLException
    {
        PrestoConnection connection = DriverManager.getConnection(
                jdbcUrl,
                queryConfiguration.getUsername().orElse(null),
                queryConfiguration.getPassword().orElse(null))
                .unwrap(PrestoConnection.class);

        try {
            connection.setClientInfo("ApplicationName", "verifier-test");
            connection.setCatalog(queryConfiguration.getCatalog());
            connection.setSchema(queryConfiguration.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }

        // configure session properties
        Map<String, String> sessionProperties = queryStage.isMain() || queryStage == DETERMINISM_ANALYSIS_MAIN
                ? new HashMap<>(queryConfiguration.getSessionProperties())
                : new HashMap<>();

        // Add or override query max execution time to enforce the timeout.
        sessionProperties.put(QUERY_MAX_EXECUTION_TIME, getTimeout(queryStage).toString());

        // Remove query max run time to respect execution time limit.
        sessionProperties.remove(QUERY_MAX_RUN_TIME);

        for (Entry<String, String> entry : sessionProperties.entrySet()) {
            connection.setSessionProperty(entry.getKey(), entry.getValue());
        }
        return connection;
    }

    private Duration getTimeout(QueryStage queryStage)
    {
        switch (queryStage) {
            case REWRITE:
            case DESCRIBE:
            case CONTROL_SETUP:
            case CONTROL_TEARDOWN:
            case TEST_SETUP:
            case TEST_TEARDOWN:
            case DETERMINISM_ANALYSIS_SETUP:
                return metadataTimeout;
            case CONTROL_CHECKSUM:
            case TEST_CHECKSUM:
            case DETERMINISM_ANALYSIS_CHECKSUM:
                return checksumTimeout;
            default:
                return queryTimeout;
        }
    }

    private static class ProgressMonitor
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

    private interface StatementExecutor<T>
    {
        T execute(PrestoStatement statement, String query)
                throws SQLException;

        ProgressMonitor getProgressMonitor();
    }

    private static class ResultConvertingStatementExecutor<R>
            implements StatementExecutor<QueryResult<R>>
    {
        private final ResultSetConverter<R> converter;
        private final ProgressMonitor progressMonitor = new ProgressMonitor();

        public ResultConvertingStatementExecutor(ResultSetConverter<R> converter)
        {
            this.converter = requireNonNull(converter, "converter is null");
        }

        @Override
        public QueryResult<R> execute(PrestoStatement statement, String query)
                throws SQLException
        {
            ImmutableList.Builder<R> rows = ImmutableList.builder();
            try (ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    converter.apply(resultSet).ifPresent(rows::add);
                }
                checkState(progressMonitor.getLastQueryStats().isPresent(), "lastQueryStats is missing");
                return new QueryResult<>(rows.build(), resultSet.getMetaData(), progressMonitor.getLastQueryStats().get());
            }
        }

        @Override
        public ProgressMonitor getProgressMonitor()
        {
            return progressMonitor;
        }
    }

    private static class NoResultStatementExecutor<R>
            implements StatementExecutor<QueryStats>
    {
        private final ProgressMonitor progressMonitor = new ProgressMonitor();

        @Override
        public QueryStats execute(PrestoStatement statement, String query)
                throws SQLException
        {
            boolean moreResults = statement.execute(query);
            if (moreResults) {
                consumeResultSet(statement.getResultSet());
            }
            do {
                moreResults = statement.getMoreResults();
                if (moreResults) {
                    consumeResultSet(statement.getResultSet());
                }
            }
            while (moreResults || statement.getUpdateCount() != -1);
            checkState(progressMonitor.getLastQueryStats().isPresent(), "lastQueryStats is missing");
            return progressMonitor.getLastQueryStats().get();
        }

        @Override
        public ProgressMonitor getProgressMonitor()
        {
            return progressMonitor;
        }

        private static void consumeResultSet(ResultSet resultSet)
                throws SQLException
        {
            while (resultSet.next()) {
                // Do nothing
            }
        }
    }
}
