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
import com.facebook.presto.connector.thrift.ThriftErrorCode;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableSet;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;

public class PrestoExceptionClassifier
        implements SqlExceptionClassifier
{
    private static final Set<ErrorCodeSupplier> DEFAULT_ERRORS = ImmutableSet.<ErrorCodeSupplier>builder()
            .addAll(asList(StandardErrorCode.values()))
            .addAll(asList(HiveErrorCode.values()))
            .addAll(asList(HiveErrorCode.values()))
            .addAll(asList(JdbcErrorCode.values()))
            .addAll(asList(ThriftErrorCode.values()))
            .build();

    private final Map<Integer, ErrorCodeSupplier> errorByCode;

    public PrestoExceptionClassifier(Set<ErrorCodeSupplier> additionalErrors)
    {
        this.errorByCode = ImmutableSet.<ErrorCodeSupplier>builder()
                .addAll(DEFAULT_ERRORS)
                .addAll(additionalErrors)
                .build()
                .stream()
                .collect(toImmutableMap(errorCode -> errorCode.toErrorCode().getCode(), identity()));
    }

    @Override
    public QueryException createException(Optional<QueryStats> queryStats, SQLException cause)
    {
        Optional<Throwable> clusterConnectionExceptionCause = getClusterConnectionExceptionCause(cause);
        if (clusterConnectionExceptionCause.isPresent()) {
            return QueryException.forClusterConnection(clusterConnectionExceptionCause.get());
        }

        Optional<ErrorCodeSupplier> errorCode = Optional.ofNullable(errorByCode.get(cause.getErrorCode()));
        return QueryException.forPresto(cause, errorCode, queryStats);
    }

    private static Optional<Throwable> getClusterConnectionExceptionCause(Throwable t)
    {
        while (t != null) {
            if (t instanceof SocketTimeoutException ||
                    t instanceof SocketException ||
                    t instanceof EOFException ||
                    t instanceof UncheckedIOException ||
                    t instanceof TimeoutException ||
                    (t.getClass().equals(RuntimeException.class) && t.getMessage() != null && t.getMessage().contains("Error fetching next at"))) {
                return Optional.of(t);
            }
            t = t.getCause();
        }
        return Optional.empty();
    }
}
