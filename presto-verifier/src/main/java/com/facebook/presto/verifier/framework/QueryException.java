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
import com.facebook.presto.verifier.event.QueryFailure;

import java.util.Optional;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public abstract class QueryException
        extends RuntimeException
{
    private final boolean retryable;
    private final QueryStage queryStage;

    public QueryException(String message, boolean retryable, QueryStage queryStage)
    {
        super(message);
        this.retryable = retryable;
        this.queryStage = requireNonNull(queryStage, "queryStage is null");
    }

    public QueryException(Throwable cause, boolean retryable, QueryStage queryStage)
    {
        super(cause);
        this.retryable = retryable;
        this.queryStage = requireNonNull(queryStage, "queryStage is null");
    }

    public abstract String getErrorCodeName();

    public boolean isRetryable()
    {
        return retryable;
    }

    public QueryStage getQueryStage()
    {
        return queryStage;
    }

    public QueryFailure toQueryFailure()
    {
        return new QueryFailure(
                queryStage,
                getErrorCodeName(),
                this instanceof PrestoQueryException
                        ? ((PrestoQueryException) this).getErrorCode().map(ErrorCodeSupplier::toErrorCode).map(ErrorCode::getType)
                        : Optional.empty(),
                retryable,
                this instanceof PrestoQueryException
                        ? ((PrestoQueryException) this).getQueryActionStats().getQueryStats().map(QueryStats::getQueryId)
                        : Optional.empty(),
                getStackTraceAsString(this));
    }
}
