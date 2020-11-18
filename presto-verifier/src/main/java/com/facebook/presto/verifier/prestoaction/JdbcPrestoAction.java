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

import com.facebook.airlift.log.Logger;
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
import com.facebook.presto.verifier.framework.ThrottlingException;
import com.facebook.presto.verifier.framework.VerificationContext;
import com.facebook.presto.verifier.framework.VerifierConfig;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.verifier.prestoaction.QueryActionUtil.mangleSessionProperties;
import static java.util.Objects.requireNonNull;

public class JdbcPrestoAction
        implements PrestoAction
{
    public static final String QUERY_ACTION_TYPE = "presto-jdbc";
    private static final Logger log = Logger.get(JdbcPrestoAction.class);

    private final SqlExceptionClassifier exceptionClassifier;
    private final QueryConfiguration queryConfiguration;
    private final VerificationContext verificationContext;
    private final Iterator<String> jdbcUrlSelector;

    private final Duration queryTimeout;
    private final Duration metadataTimeout;
    private final Duration checksumTimeout;
    private final String testId;
    private final Optional<String> testName;
    private final String applicationName;
    private final boolean removeMemoryRelatedSessionProperties;

    private final RetryDriver<QueryException> networkRetry;
    private final RetryDriver<QueryException> prestoRetry;

    public JdbcPrestoAction(
            SqlExceptionClassifier exceptionClassifier,
            QueryConfiguration queryConfiguration,
            VerificationContext verificationContext,
            Iterator<String> jdbcUrlSelector,
            PrestoActionConfig prestoActionConfig,
            Duration metadataTimeout,
            Duration checksumTimeout,
            @ForClusterConnection RetryConfig networkRetryConfig,
            @ForPresto RetryConfig prestoRetryConfig,
            VerifierConfig verifierConfig)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.queryConfiguration = requireNonNull(queryConfiguration, "queryConfiguration is null");
        this.verificationContext = requireNonNull(verificationContext, "verificationContext is null");
        this.jdbcUrlSelector = requireNonNull(jdbcUrlSelector, "jdbcUrlSelector is null");

        this.queryTimeout = requireNonNull(prestoActionConfig.getQueryTimeout(), "queryTimeout is null");
        this.applicationName = requireNonNull(prestoActionConfig.getApplicationName(), "applicationName is null");
        this.removeMemoryRelatedSessionProperties = prestoActionConfig.isRemoveMemoryRelatedSessionProperties();
        this.metadataTimeout = requireNonNull(metadataTimeout, "metadataTimeout is null");
        this.checksumTimeout = requireNonNull(checksumTimeout, "checksumTimeout is null");
        this.testId = requireNonNull(verifierConfig.getTestId(), "testId is null");
        this.testName = requireNonNull(verifierConfig.getTestName(), "testName is null");

        this.networkRetry = new RetryDriver<>(
                networkRetryConfig,
                queryException -> (queryException instanceof ClusterConnectionException || queryException instanceof ThrottlingException) && queryException.isRetryable(),
                QueryException.class,
                verificationContext::addException);
        this.prestoRetry = new RetryDriver<>(
                prestoRetryConfig,
                queryException -> queryException instanceof PrestoQueryException && queryException.isRetryable(),
                QueryException.class,
                verificationContext::addException);
    }

    @Override
    public QueryActionStats execute(Statement statement, QueryStage queryStage)
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
        String clientInfo = new ClientInfo(
                testId,
                testName,
                verificationContext.getSourceQueryName(),
                verificationContext.getSuite()).serialize();

        try (PrestoConnection connection = getConnection(queryStage, clientInfo)) {
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

    private PrestoConnection getConnection(QueryStage queryStage, String clientInfo)
            throws SQLException
    {
        PrestoConnection connection = DriverManager.getConnection(
                jdbcUrlSelector.next(),
                queryConfiguration.getUsername().orElse(null),
                queryConfiguration.getPassword().orElse(null))
                .unwrap(PrestoConnection.class);

        try {
            connection.setClientInfo("ApplicationName", applicationName);
            connection.setClientInfo("ClientInfo", clientInfo);
            connection.setCatalog(queryConfiguration.getCatalog());
            connection.setSchema(queryConfiguration.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }

        Map<String, String> sessionProperties = mangleSessionProperties(
                queryConfiguration.getSessionProperties(),
                queryStage,
                getTimeout(queryStage),
                removeMemoryRelatedSessionProperties);
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
            if (!this.queryStats.isPresent()) {
                log.debug("Running Presto Query: %s", queryStats.getQueryId());
            }
            this.queryStats = Optional.of(requireNonNull(queryStats, "queryStats is null"));
        }

        public synchronized QueryActionStats getLastQueryStats()
        {
            return new QueryActionStats(queryStats, Optional.empty());
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
                return new QueryResult<>(rows.build(), resultSet.getMetaData(), progressMonitor.getLastQueryStats());
            }
        }

        @Override
        public ProgressMonitor getProgressMonitor()
        {
            return progressMonitor;
        }
    }

    private static class NoResultStatementExecutor<R>
            implements StatementExecutor<QueryActionStats>
    {
        private final ProgressMonitor progressMonitor = new ProgressMonitor();

        @Override
        public QueryActionStats execute(PrestoStatement statement, String query)
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
            return progressMonitor.getLastQueryStats();
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
