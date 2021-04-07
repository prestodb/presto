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

import com.facebook.presto.benchmark.framework.QueryException;
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

import static com.facebook.presto.benchmark.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.benchmark.framework.QueryException.Type.PRESTO;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPrestoExceptionClassifier
{
    private static final QueryStats QUERY_STATS = new QueryStats("id", "", false, false, false, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, Optional.empty());

    private final SqlExceptionClassifier classifier = new PrestoExceptionClassifier(ImmutableSet.of());

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
                classifier.createException(Optional.empty(), sqlException),
                CLUSTER_CONNECTION,
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void assertPrestoException()
    {
        assertPrestoException(NO_NODES_AVAILABLE);
        assertPrestoException(SERVER_STARTING_UP);
        assertPrestoException(HIVE_CANNOT_OPEN_SPLIT);

        assertPrestoException(SUBQUERY_MULTIPLE_ROWS);
        assertPrestoException(FUNCTION_IMPLEMENTATION_ERROR);
        assertPrestoException(EXCEEDED_TIME_LIMIT);
        assertPrestoException(HIVE_CORRUPTED_COLUMN_STATISTICS);
    }

    private void assertPrestoException(ErrorCodeSupplier errorCode)
    {
        SQLException sqlException = new SQLException("", "", errorCode.toErrorCode().getCode(), new PrestoException(errorCode, errorCode.toErrorCode().getName()));
        assertQueryException(
                classifier.createException(Optional.of(QUERY_STATS), sqlException),
                PRESTO,
                Optional.of(errorCode),
                Optional.of(QUERY_STATS));
    }

    @Test
    public void testUnknownPrestoException()
    {
        SQLException sqlException = new SQLException("", "", 0xabcd_1234, new RuntimeException());
        assertQueryException(
                classifier.createException(Optional.of(QUERY_STATS), sqlException),
                PRESTO,
                Optional.empty(),
                Optional.of(QUERY_STATS));
    }

    private void assertQueryException(
            QueryException queryException,
            QueryException.Type type,
            Optional<ErrorCodeSupplier> prestoErrorCode,
            Optional<QueryStats> queryStats)
    {
        assertEquals(queryException.getType(), type);
        assertEquals(queryException.getPrestoErrorCode(), prestoErrorCode);
        assertEquals(queryException.getQueryStats(), queryStats);
    }
}
