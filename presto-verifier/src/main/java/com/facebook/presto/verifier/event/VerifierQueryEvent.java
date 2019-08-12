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

import com.facebook.presto.verifier.framework.SkippedReason;
import com.google.common.collect.ImmutableList;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("VerifierQuery")
public class VerifierQueryEvent
{
    public enum EventStatus
    {
        SUCCEEDED,
        FAILED,
        FAILED_RESOLVED,
        SKIPPED,
    }

    private final String suite;
    private final String testId;
    private final String name;

    private final String status;
    private final String skippedReason;

    private final Boolean deterministic;
    private final String resolveMessage;

    private final QueryInfo controlQueryInfo;
    private final QueryInfo testQueryInfo;

    private final String errorCode;
    private final String errorMessage;

    private final QueryFailure finalQueryFailure;
    private final List<QueryFailure> queryFailures;

    public VerifierQueryEvent(
            String suite,
            String testId,
            String name,
            EventStatus status,
            Optional<SkippedReason> skippedReason,
            Optional<Boolean> deterministic,
            Optional<String> resolveMessage,
            QueryInfo controlQueryInfo,
            QueryInfo testQueryInfo,
            Optional<String> errorCode,
            Optional<String> errorMessage,
            Optional<QueryFailure> finalQueryFailure,
            List<QueryFailure> queryFailures)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.testId = requireNonNull(testId, "testId is null");
        this.name = requireNonNull(name, "name is null");
        this.status = status.name();
        this.skippedReason = skippedReason.map(SkippedReason::name).orElse(null);
        this.deterministic = deterministic.orElse(null);
        this.resolveMessage = resolveMessage.orElse(null);
        this.controlQueryInfo = requireNonNull(controlQueryInfo, "controlQueryInfo is null");
        this.testQueryInfo = requireNonNull(testQueryInfo, "testQueryInfo is null");
        this.errorCode = errorCode.orElse(null);
        this.errorMessage = errorMessage.orElse(null);
        this.finalQueryFailure = finalQueryFailure.orElse(null);
        this.queryFailures = ImmutableList.copyOf(queryFailures);
    }

    @EventField
    public String getSuite()
    {
        return suite;
    }

    @EventField
    public String getTestId()
    {
        return testId;
    }

    @EventField
    public String getName()
    {
        return name;
    }

    @EventField
    public String getStatus()
    {
        return status;
    }

    @EventField
    public String getSkippedReason()
    {
        return skippedReason;
    }

    @EventField
    public Boolean getDeterministic()
    {
        return deterministic;
    }

    @EventField
    public String getResolveMessage()
    {
        return resolveMessage;
    }

    @EventField
    public QueryInfo getControlQueryInfo()
    {
        return controlQueryInfo;
    }

    @EventField
    public QueryInfo getTestQueryInfo()
    {
        return testQueryInfo;
    }

    @EventField
    public String getErrorCode()
    {
        return errorCode;
    }

    @EventField
    public String getErrorMessage()
    {
        return errorMessage;
    }

    @EventField
    public QueryFailure getFinalQueryFailure()
    {
        return finalQueryFailure;
    }

    @EventField
    public List<QueryFailure> getQueryFailures()
    {
        return queryFailures;
    }
}
