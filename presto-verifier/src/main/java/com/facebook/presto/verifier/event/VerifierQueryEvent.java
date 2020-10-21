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
import com.facebook.presto.verifier.framework.SkippedReason;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.verifier.event.VerifierQueryEvent.EventStatus.SKIPPED;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
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
    private final String matchType;
    private final String skippedReason;

    private final String determinismAnalysis;
    private final DeterminismAnalysisDetails determinismAnalysisDetails;
    private final String resolveMessage;

    private final QueryInfo controlQueryInfo;
    private final QueryInfo testQueryInfo;

    private final String errorCode;
    private final String errorMessage;

    private final QueryFailure finalQueryFailure;
    private final List<QueryFailure> queryFailures;

    private final int resubmissionCount;

    public VerifierQueryEvent(
            String suite,
            String testId,
            String name,
            EventStatus status,
            Optional<String> matchType,
            Optional<SkippedReason> skippedReason,
            Optional<DeterminismAnalysisDetails> determinismAnalysisDetails,
            Optional<String> resolveMessage,
            Optional<QueryInfo> controlQueryInfo,
            QueryInfo testQueryInfo,
            Optional<String> errorCode,
            Optional<String> errorMessage,
            Optional<QueryFailure> finalQueryFailure,
            List<QueryFailure> queryFailures,
            int resubmissionCount)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.testId = requireNonNull(testId, "testId is null");
        this.name = requireNonNull(name, "name is null");
        this.status = status.name();
        this.matchType = matchType.orElse(null);
        this.skippedReason = skippedReason.map(SkippedReason::name).orElse(null);
        this.determinismAnalysis = determinismAnalysisDetails.map(DeterminismAnalysisDetails::getResult).orElse(null);
        this.determinismAnalysisDetails = determinismAnalysisDetails.orElse(null);
        this.resolveMessage = resolveMessage.orElse(null);
        this.controlQueryInfo = controlQueryInfo.orElse(null);
        this.testQueryInfo = requireNonNull(testQueryInfo, "testQueryInfo is null");
        this.errorCode = errorCode.orElse(null);
        this.errorMessage = errorMessage.orElse(null);
        this.finalQueryFailure = finalQueryFailure.orElse(null);
        this.queryFailures = ImmutableList.copyOf(queryFailures);
        this.resubmissionCount = resubmissionCount;
    }

    public static VerifierQueryEvent skipped(
            String suite,
            String testId,
            SourceQuery sourceQuery,
            SkippedReason skippedReason,
            boolean skipControl)
    {
        return new VerifierQueryEvent(
                suite,
                testId,
                sourceQuery.getName(),
                SKIPPED,
                Optional.empty(),
                Optional.of(skippedReason),
                Optional.empty(),
                Optional.empty(),
                skipControl ?
                        Optional.empty() :
                        Optional.of(QueryInfo.builder(
                                sourceQuery.getControlConfiguration().getCatalog(),
                                sourceQuery.getControlConfiguration().getSchema(),
                                sourceQuery.getQuery(CONTROL),
                                sourceQuery.getControlConfiguration().getSessionProperties()).build()),
                QueryInfo.builder(
                        sourceQuery.getTestConfiguration().getCatalog(),
                        sourceQuery.getTestConfiguration().getSchema(),
                        sourceQuery.getQuery(TEST),
                        sourceQuery.getTestConfiguration().getSessionProperties()).build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                0);
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
    public String getMatchType()
    {
        return matchType;
    }

    @EventField
    public String getSkippedReason()
    {
        return skippedReason;
    }

    @EventField
    public String getDeterminismAnalysis()
    {
        return determinismAnalysis;
    }

    @EventField
    public DeterminismAnalysisDetails getDeterminismAnalysisDetails()
    {
        return determinismAnalysisDetails;
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

    @EventField
    public int getResubmissionCount()
    {
        return resubmissionCount;
    }
}
