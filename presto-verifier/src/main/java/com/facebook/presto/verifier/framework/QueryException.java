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
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.google.common.base.Function;

import java.util.Optional;

import static com.facebook.presto.verifier.framework.QueryException.Type.CLUSTER_CONNECTION;
import static com.facebook.presto.verifier.framework.QueryException.Type.PRESTO;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryException
        extends RuntimeException
{
    public enum Type
    {
        CLUSTER_CONNECTION(qe -> {
            requireNonNull(qe, "queryException is null");
            requireNonNull(qe.getCause(), "cause is null");
            return qe.getCause().getClass().getSimpleName();
        }),
        PRESTO(qe -> {
            requireNonNull(qe, "queryException is null");
            return qe.prestoErrorCode.map(ErrorCodeSupplier::toErrorCode).map(ErrorCode::getName).orElse("UNKNOWN");
        });

        private final Function<QueryException, String> descriptionGenerator;

        Type(Function<QueryException, String> descriptionGenerator)
        {
            this.descriptionGenerator = requireNonNull(descriptionGenerator, "descriptionGenerator is null");
        }
    }

    private final Type type;
    private final Optional<ErrorCodeSupplier> prestoErrorCode;
    private final boolean retryable;
    private final Optional<QueryStats> queryStats;
    private final QueryOrigin queryOrigin;

    private QueryException(
            Throwable cause,
            Type type,
            Optional<ErrorCodeSupplier> prestoErrorCode,
            boolean retryable,
            Optional<QueryStats> queryStats,
            QueryOrigin queryOrigin)
    {
        super(cause);
        this.type = requireNonNull(type, "type is null");
        this.prestoErrorCode = requireNonNull(prestoErrorCode, "errorCode is null");
        this.retryable = retryable;
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
        this.queryOrigin = requireNonNull(queryOrigin, "queryOrigin is null");
    }

    public static QueryException forClusterConnection(Throwable cause, QueryOrigin queryOrigin)
    {
        return new QueryException(cause, CLUSTER_CONNECTION, Optional.empty(), true, Optional.empty(), queryOrigin);
    }

    public static QueryException forPresto(Throwable cause, Optional<ErrorCodeSupplier> prestoErrorCode, boolean retryable, Optional<QueryStats> queryStats, QueryOrigin queryOrigin)
    {
        return new QueryException(cause, PRESTO, prestoErrorCode, retryable, queryStats, queryOrigin);
    }

    public Type getType()
    {
        return type;
    }

    public Optional<ErrorCodeSupplier> getPrestoErrorCode()
    {
        return prestoErrorCode;
    }

    public boolean isRetryable()
    {
        return retryable;
    }

    public Optional<QueryStats> getQueryStats()
    {
        return queryStats;
    }

    public QueryOrigin getQueryOrigin()
    {
        return queryOrigin;
    }

    public String getErrorCode()
    {
        return format("%s(%s)", type.name(), type.descriptionGenerator.apply(this));
    }
}
