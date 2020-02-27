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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcPrestoAction
        implements PrestoAction
{
    private static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";

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
        return execute(statement, queryStage, Optional.empty()).getQueryStats();
    }

    @Override
    public <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, ResultSetConverter<R> converter)
    {
        return execute(statement, queryStage, Optional.of(converter));
    }

    private <R> QueryResult<R> execute(Statement statement, QueryStage queryStage, Optional<ResultSetConverter<R>> converter)
    {
        return prestoRetry.run(
                "presto",
                () -> networkRetry.run(
                        "presto-cluster-connection",
                        () -> executeOnce(statement, queryStage, converter)));
    }

    private <R> QueryResult<R> executeOnce(Statement statement, QueryStage queryStage, Optional<ResultSetConverter<R>> converter)
    {
        String query = formatSql(statement, Optional.empty());
        ProgressMonitor progressMonitor = new ProgressMonitor();

        try (PrestoConnection connection = getConnection(queryStage)) {
            try (java.sql.Statement jdbcStatement = connection.createStatement()) {
                PrestoStatement prestoStatement = jdbcStatement.unwrap(PrestoStatement.class);
                prestoStatement.setProgressMonitor(progressMonitor);

                ImmutableList.Builder<R> rows = ImmutableList.builder();
                ImmutableList.Builder<String> columnNames = ImmutableList.builder();
                if (converter.isPresent()) {
                    try (ResultSet resultSet = jdbcStatement.executeQuery(query)) {
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            columnNames.add(resultSet.getMetaData().getColumnName(i));
                        }
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
                return new QueryResult<>(rows.build(), columnNames.build(), progressMonitor.getLastQueryStats().get());
            }
        }
        catch (SQLException e) {
            throw exceptionClassifier.createException(queryStage, progressMonitor.getLastQueryStats(), e);
        }
    }

    private void consumeResultSet(ResultSet resultSet)
            throws SQLException
    {
        while (resultSet.next()) {
            // Do nothing
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

        Map<String, String> sessionProperties = ImmutableMap.<String, String>builder()
                .putAll(queryConfiguration.getSessionProperties())
                .put(QUERY_MAX_EXECUTION_TIME, getTimeout(queryStage).toString())
                .build();
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
                return metadataTimeout;
            case CHECKSUM:
                return checksumTimeout;
            default:
                return queryTimeout;
        }
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
