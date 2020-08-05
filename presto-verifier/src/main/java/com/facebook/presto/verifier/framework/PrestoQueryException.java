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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoQueryException
        extends QueryException
{
    private final Optional<ErrorCodeSupplier> errorCode;
    private final QueryActionStats queryActionStats;

    public PrestoQueryException(
            String message,
            boolean retryable,
            QueryStage queryStage,
            Optional<ErrorCodeSupplier> errorCode,
            QueryActionStats queryActionStats)
    {
        super(message, retryable, queryStage);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.queryActionStats = requireNonNull(queryActionStats, "queryActionStats is null");
    }

    public PrestoQueryException(
            Throwable cause,
            boolean retryable,
            QueryStage queryStage,
            Optional<ErrorCodeSupplier> errorCode,
            QueryActionStats queryActionStats)
    {
        super(cause, retryable, queryStage);
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.queryActionStats = requireNonNull(queryActionStats, "queryActionStats is null");
    }

    public Optional<ErrorCodeSupplier> getErrorCode()
    {
        return errorCode;
    }

    public QueryActionStats getQueryActionStats()
    {
        return queryActionStats;
    }

    @Override
    public String getErrorCodeName()
    {
        return format("PRESTO(%s)", errorCode.map(ErrorCodeSupplier::toErrorCode).map(ErrorCode::getName).orElse("UNKNOWN"));
    }
}
