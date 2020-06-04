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

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.verifier.framework.ClusterConnectionException;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryStage;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_SETUP;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_SETUP;
import static com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier.shouldResubmit;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPrestoExceptionClassifier
{
    private static final QueryStage QUERY_STAGE = CONTROL_MAIN;
    private static final QueryStats QUERY_STATS = new QueryStats("id", "", false, false, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, Optional.empty());

    private final SqlExceptionClassifier classifier = PrestoExceptionClassifier.defaultBuilder().build();

    @Test
    public void testNetworkException()
    {
        testNetworkException(new SQLException(new SocketTimeoutException()));
        testNetworkException(new SQLException(new SocketException()));
        testNetworkException(new SQLException(new ConnectException()));
        testNetworkException(new SQLException(new EOFException()));
        testNetworkException(new SQLException(new UncheckedIOException(new IOException())));
        testNetworkException(new SQLException(new RuntimeException("Error fetching next at")));
    }

    private void testNetworkException(SQLException sqlException)
    {
        assertClusterConnectionException(classifier.createException(QUERY_STAGE, Optional.empty(), sqlException), QUERY_STAGE);
    }

    @Test
    public void testPrestoException()
    {
        testPrestoException(NO_NODES_AVAILABLE, true);
        testPrestoException(SERVER_STARTING_UP, true);
        testPrestoException(HIVE_CANNOT_OPEN_SPLIT, true);

        testPrestoException(SUBQUERY_MULTIPLE_ROWS, false);
        testPrestoException(FUNCTION_IMPLEMENTATION_ERROR, false);
        testPrestoException(EXCEEDED_TIME_LIMIT, false);
        testPrestoException(HIVE_CORRUPTED_COLUMN_STATISTICS, false);
    }

    private void testPrestoException(ErrorCodeSupplier errorCode, boolean expectedRetryable)
    {
        SQLException sqlException = new SQLException("", "", errorCode.toErrorCode().getCode(), new PrestoException(errorCode, errorCode.toErrorCode().getName()));
        assertPrestoQueryException(
                classifier.createException(QUERY_STAGE, Optional.of(QUERY_STATS), sqlException),
                Optional.of(errorCode),
                expectedRetryable,
                Optional.of(QUERY_STATS),
                QUERY_STAGE);
    }

    @Test
    public void testUnknownPrestoException()
    {
        SQLException sqlException = new SQLException("", "", 0xabcd_1234, new RuntimeException());
        assertPrestoQueryException(
                classifier.createException(QUERY_STAGE, Optional.of(QUERY_STATS), sqlException),
                Optional.empty(),
                false,
                Optional.of(QUERY_STATS),
                QUERY_STAGE);
    }

    @Test
    public void testTargetTableAlreadyExists()
    {
        String message = "Table 'a.b.c' already exists";
        SQLException sqlException = new SQLException(message, "", SYNTAX_ERROR.toErrorCode().getCode(), new PrestoException(SYNTAX_ERROR, message));

        assertTrue(shouldResubmit(classifier.createException(CONTROL_SETUP, Optional.of(QUERY_STATS), sqlException)));
        assertTrue(shouldResubmit(classifier.createException(TEST_SETUP, Optional.of(QUERY_STATS), sqlException)));
        assertFalse(shouldResubmit(classifier.createException(CONTROL_MAIN, Optional.of(QUERY_STATS), sqlException)));
    }

    private void assertClusterConnectionException(QueryException queryException, QueryStage queryStage)
    {
        assertTrue(queryException instanceof ClusterConnectionException);
        assertEquals(queryException.getQueryStage(), queryStage);
        ClusterConnectionException exception = (ClusterConnectionException) queryException;

        assertTrue(exception.isRetryable());
    }

    private void assertPrestoQueryException(
            QueryException queryException,
            Optional<ErrorCodeSupplier> errorCode,
            boolean retryable,
            Optional<QueryStats> queryStats,
            QueryStage queryStage)
    {
        assertTrue(queryException instanceof PrestoQueryException);
        assertEquals(queryException.getQueryStage(), queryStage);
        PrestoQueryException exception = (PrestoQueryException) queryException;

        assertEquals(exception.getErrorCode(), errorCode);
        assertEquals(exception.isRetryable(), retryable);
        assertEquals(exception.getQueryStats(), queryStats);
    }
}
