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

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
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
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.verifier.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.verifier.framework.QueryException.Type.PRESTO;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_MAIN;

public class TestPrestoExceptionClassifier
{
    private static final QueryStage QUERY_STAGE = CONTROL_MAIN;
    private static final QueryStats QUERY_STATS = new QueryStats("id", "", false, false, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, Optional.empty());

    private final SqlExceptionClassifier classifier = new PrestoExceptionClassifier(ImmutableSet.of(), ImmutableSet.of());

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
        assertQueryException(
                classifier.createException(QUERY_STAGE, Optional.empty(), sqlException),
                CLUSTER_CONNECTION,
                Optional.empty(),
                true,
                Optional.empty(),
                QUERY_STAGE);
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
        assertQueryException(
                classifier.createException(QUERY_STAGE, Optional.of(QUERY_STATS), sqlException),
                PRESTO,
                Optional.of(errorCode),
                expectedRetryable,
                Optional.of(QUERY_STATS),
                QUERY_STAGE);
    }

    @Test
    public void testUnknownPrestoException()
    {
        SQLException sqlException = new SQLException("", "", 0xabcd_1234, new RuntimeException());
        assertQueryException(
                classifier.createException(QUERY_STAGE, Optional.of(QUERY_STATS), sqlException),
                PRESTO,
                Optional.empty(),
                false,
                Optional.of(QUERY_STATS),
                QUERY_STAGE);
    }

    private void assertQueryException(
            QueryException queryException,
            QueryException.Type type,
            Optional<ErrorCodeSupplier> prestoErrorCode,
            boolean retryable,
            Optional<QueryStats> queryStats,
            QueryStage queryStage)
    {
        assertEquals(queryException.getType(), type);
        assertEquals(queryException.getPrestoErrorCode(), prestoErrorCode);
        assertEquals(queryException.isRetryable(), retryable);
        assertEquals(queryException.getQueryStats(), queryStats);
        assertEquals(queryException.getQueryStage(), queryStage);
    }
}
