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

import com.facebook.presto.verifier.framework.QueryOrigin.QueryStage;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("QueryInfo")
public class FailureInfo
{
    private final String queryStage;
    private final String errorCode;
    private final String prestoQueryId;
    private final String stacktrace;

    public FailureInfo(QueryStage queryStage, String errorCode, Optional<String> prestoQueryId, String stacktrace)
    {
        this.queryStage = queryStage.name();
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.prestoQueryId = prestoQueryId.orElse(null);
        this.stacktrace = requireNonNull(stacktrace, "stacktrace is null");
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
