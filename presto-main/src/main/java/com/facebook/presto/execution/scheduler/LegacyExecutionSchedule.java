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

import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageState;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.facebook.presto.execution.StageState.RUNNING;
import static com.facebook.presto.execution.StageState.SCHEDULED;
import static java.util.Objects.requireNonNull;

public class LegacyExecutionSchedule
        implements ExecutionSchedule
{
    private final Set<SqlStageExecution> schedulingStages;

    public LegacyExecutionSchedule(Collection<SqlStageExecution> stages)
    {
        this.schedulingStages = new HashSet<>(requireNonNull(stages, "stages is null"));
    }

    @Override
    public Set<SqlStageExecution> getStagesToSchedule()
    {
        for (Iterator<SqlStageExecution> iterator = schedulingStages.iterator(); iterator.hasNext(); ) {
            StageState state = iterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                iterator.remove();
            }

        }
        return schedulingStages;
    }

    @Override
    public boolean isFinished()
    {
        return schedulingStages.isEmpty();
    }
}
