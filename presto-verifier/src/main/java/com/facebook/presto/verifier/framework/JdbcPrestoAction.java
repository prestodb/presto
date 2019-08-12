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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.Statement;
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
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.facebook.presto.verifier.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.verifier.framework.QueryException.Type.PRESTO;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class JdbcPrestoAction
        implements PrestoAction
{
    private static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";

    private final SqlExceptionClassifier exceptionClassifier;
    private final Map<ClusterType, QueryConfiguration> configurations;
    private final VerificationContext context;

    private final Map<ClusterType, String> clusterUrls;
    private final Duration controlTimeout;
    private final Duration testTimeout;
    private final Duration metadataTimeout;
    private final Duration checksumTimeout;

    private final RetryDriver networkRetry;
    private final RetryDriver prestoRetry;

    public JdbcPrestoAction(
            SqlExceptionClassifier exceptionClassifier,
            QueryConfiguration controlConfiguration,
            QueryConfiguration testConfiguration,
            VerificationContext context,
            VerifierConfig config,
            @ForClusterConnection RetryConfig networkRetryConfig,
            @ForPresto RetryConfig prestoRetryConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.configurations = ImmutableMap.of(
                CONTROL, controlConfiguration,
                TEST, testConfiguration);
        this.context = requireNonNull(context, "context is null");

        this.clusterUrls = ImmutableMap.of(
                CONTROL, config.getControlJdbcUrl(),
                TEST, config.getTestJdbcUrl());
        this.controlTimeout = requireNonNull(config.getControlTimeout(), "controlTimeout is null");
        this.testTimeout = requireNonNull(config.getTestTimeout(), "testTimeout is null");
        this.metadataTimeout = requireNonNull(config.getMetadataTimeout(), "metadataTimeout is null");
        this.checksumTimeout = requireNonNull(config.getChecksumTimeout(), "checksumTimeout is null");

        this.networkRetry = new RetryDriver(networkRetryConfig, queryException -> queryException.getType() == CLUSTER_CONNECTION);
        this.prestoRetry = new RetryDriver(prestoRetryConfig, queryException -> queryException.getType() == PRESTO && queryException.isRetryable());
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
                context,
                () -> networkRetry.run(
                        "presto-cluster-connection",
                        context,
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
        QueryConfiguration configuration = configurations.get(queryStage.getTargetCluster());
        PrestoConnection connection = DriverManager.getConnection(
                clusterUrls.get(queryStage.getTargetCluster()),
                configuration.getUsername().orElse(null),
                configuration.getPassword().orElse(null))
                .unwrap(PrestoConnection.class);

        try {
            connection.setClientInfo("ApplicationName", "verifier-test");
            connection.setCatalog(configuration.getCatalog());
            connection.setSchema(configuration.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }

        Map<String, String> sessionProperties = ImmutableMap.<String, String>builder()
                .putAll(configuration.getSessionProperties())
                .put(QUERY_MAX_EXECUTION_TIME, getTimeout(queryStage).toString())
                .build();
        for (Entry<String, String> entry : sessionProperties.entrySet()) {
            connection.setSessionProperty(entry.getKey(), entry.getValue());
        }
        return connection;
    }

    private Duration getTimeout(QueryStage queryStage)
    {
        ClusterType cluster = queryStage.getTargetCluster();
        checkState(cluster == CONTROL || cluster == TEST, "Invalid cluster: %s", cluster);

        switch (queryStage) {
            case REWRITE:
            case DESCRIBE:
                return metadataTimeout;
            case CHECKSUM:
                return checksumTimeout;
            default:
                return cluster == CONTROL ? controlTimeout : testTimeout;
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
