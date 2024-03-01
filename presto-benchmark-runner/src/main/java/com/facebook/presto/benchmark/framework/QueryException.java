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
package com.facebook.presto.benchmark.framework;

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;

import java.util.Optional;

import static com.facebook.presto.benchmark.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.benchmark.framework.QueryException.Type.PRESTO;
import static java.util.Objects.requireNonNull;

public class QueryException
        extends RuntimeException
{
    public enum Type
    {
        CLUSTER_CONNECTION,
        PRESTO
    }

    private final Type type;
    private final Optional<ErrorCodeSupplier> prestoErrorCode;
    private final Optional<QueryStats> queryStats;

    public QueryException(
            Throwable cause,
            Type type,
            Optional<ErrorCodeSupplier> prestoErrorCode,
            Optional<QueryStats> queryStats)
    {
        super(cause);
        this.type = requireNonNull(type, "type is null");
        this.prestoErrorCode = requireNonNull(prestoErrorCode, "errorCode is null");
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
    }

    public static QueryException forClusterConnection(Throwable cause)
    {
        return new QueryException(cause, CLUSTER_CONNECTION, Optional.empty(), Optional.empty());
    }

    public static QueryException forPresto(Throwable cause, Optional<ErrorCodeSupplier> prestoErrorCode, Optional<QueryStats> queryStats)
    {
        return new QueryException(cause, PRESTO, prestoErrorCode, queryStats);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<ErrorCodeSupplier> getPrestoErrorCode()
    {
        return prestoErrorCode;
    }

    public Optional<QueryStats> getQueryStats()
    {
        return queryStats;
    }
}
