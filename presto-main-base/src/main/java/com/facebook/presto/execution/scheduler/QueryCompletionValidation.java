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
import com.facebook.presto.sql.planner.PlanFragment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
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
            session.getRuntimeStats().addMetricValue("queryCompletionValidationEnabled", NONE, 1);
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
        if (!isEnabled() || stageInfo.getState().isFailure()) {
            // The stage-state listener owns query failure. Do not invoke the
            // validators or let a failed stage satisfy the finishing guard.
            return;
        }
        long start = System.nanoTime();
        try {
            validators.forEach(validator -> validator.validate(session, fragment, stageInfo));
            if (validatedStages.add(stageId)) {
                session.getRuntimeStats().addMetricValue("queryCompletionValidationStages", NONE, 1);
            }
        }
        catch (Throwable failure) {
            session.getRuntimeStats().addMetricValue("queryCompletionValidationFailures", NONE, 1);
            throw failure;
        }
        finally {
            session.getRuntimeStats().addMetricValue("queryCompletionValidationWallNanos", NANO, System.nanoTime() - start);
        }
    }

    public boolean tryFinish(Set<StageExecutionId> stages)
    {
        // Final-stage callbacks run asynchronously after stage state changes.
        // Validation must finish before FINISHING starts transaction commit.
        return outputFinished.get() && (!isEnabled() || validatedStages.containsAll(stages)) &&
                finishing.compareAndSet(false, true);
    }
}
