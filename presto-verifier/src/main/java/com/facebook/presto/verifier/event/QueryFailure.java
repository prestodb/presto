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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.verifier.framework.QueryStage;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("QueryFailure")
public class QueryFailure
{
    private final String clusterType;
    private final String queryStage;
    private final String errorCode;
    private final String errorType;
    private final boolean retryable;
    private final String prestoQueryId;
    private final String stacktrace;

    public QueryFailure(
            QueryStage queryStage,
            String errorCode,
            Optional<ErrorType> errorType,
            boolean retryable,
            Optional<String> prestoQueryId,
            String stacktrace)
    {
        this.queryStage = requireNonNull(queryStage.name(), "queryStage is null");
        this.clusterType = requireNonNull(queryStage.getTargetCluster().name(), "cluster is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.errorType = errorType.map(ErrorType::name).orElse(null);
        this.retryable = retryable;
        this.prestoQueryId = prestoQueryId.orElse(null);
        this.stacktrace = requireNonNull(stacktrace, "stacktrace is null");
    }

    @EventField
    public String getClusterType()
    {
        return clusterType;
    }

    @EventField
    public String getQueryStage()
    {
        return queryStage;
    }

    @EventField
    public String getErrorCode()
    {
        return errorCode;
    }

    @EventField
    public String getErrorType()
    {
        return errorType;
    }

    @EventField
    public boolean isRetryable()
    {
        return retryable;
    }

    @EventField
    public String getPrestoQueryId()
    {
        return prestoQueryId;
    }

    @EventField
    public String getStacktrace()
    {
        return stacktrace;
    }
}
