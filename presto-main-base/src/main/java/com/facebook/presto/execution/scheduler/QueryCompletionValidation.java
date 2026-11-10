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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryCompletionValidator;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.sql.planner.PlanFragment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_ENABLED;
import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_FAILURES;
import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_SKIPPED;
import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_STAGES;
import static com.facebook.presto.common.RuntimeMetricName.QUERY_COMPLETION_VALIDATION_WALL_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class QueryCompletionValidation
{
    private final Session session;
    private final List<QueryCompletionValidator> validators;
    private final Set<StageExecutionId> validatedStages = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean outputFinished = new AtomicBoolean();
    private final AtomicBoolean finishing = new AtomicBoolean();

    public QueryCompletionValidation(Session session, Set<QueryCompletionValidator> validators)
    {
        this.session = requireNonNull(session, "session is null");
        this.validators = requireNonNull(validators, "validators is null").stream()
                .filter(validator -> validator.isEnabled(session))
                .collect(toImmutableList());
        if (isEnabled()) {
            session.getRuntimeStats().addMetricValue(QUERY_COMPLETION_VALIDATION_ENABLED, NONE, 1);
        }
    }

    public boolean isEnabled()
    {
        return !validators.isEmpty();
    }

    public void markOutputFinished()
    {
        outputFinished.set(true);
    }

    public boolean isOutputFinished()
    {
        return outputFinished.get();
    }

    /** Called with final information from the existing final-stage-info listener. */
    public void validateStage(StageExecutionId stageId, PlanFragment fragment, StageExecutionInfo stageInfo)
    {
        if (!isEnabled() || (stageInfo != null && stageInfo.getState().isFailure())) {
            // The stage-state listener owns query failure. Do not invoke the
            // validators or let a failed stage satisfy the finishing guard.
            return;
        }
        if (fragment == null || stageInfo == null || !stageInfo.isFinal()) {
            recordSkippedValidation(session, "final information is unavailable for stage " + stageId);
            validatedStages.add(stageId);
            return;
        }
        long start = System.nanoTime();
        try {
            validators.forEach(validator -> validator.validate(session, fragment, stageInfo));
            if (validatedStages.add(stageId)) {
                session.getRuntimeStats().addMetricValue(QUERY_COMPLETION_VALIDATION_STAGES, NONE, 1);
            }
        }
        catch (Throwable failure) {
            session.getRuntimeStats().addMetricValue(QUERY_COMPLETION_VALIDATION_FAILURES, NONE, 1);
            throw failure;
        }
        finally {
            session.getRuntimeStats().addMetricValue(QUERY_COMPLETION_VALIDATION_WALL_NANOS, NANO, System.nanoTime() - start);
        }
    }

    public boolean tryFinish(Set<StageExecutionId> stages)
    {
        // Final-stage callbacks run asynchronously after stage state changes.
        // Validation must finish before FINISHING starts transaction commit.
        return outputFinished.get() && (!isEnabled() || validatedStages.containsAll(stages)) &&
                finishing.compareAndSet(false, true);
    }

    public static void recordSkippedValidation(Session session, String reason)
    {
        session.getRuntimeStats().addMetricValue(QUERY_COMPLETION_VALIDATION_SKIPPED, NONE, 1);
        session.getWarningCollector().add(new PrestoWarning(SEMANTIC_WARNING, "Skipped query completion validation: " + reason));
    }
}
