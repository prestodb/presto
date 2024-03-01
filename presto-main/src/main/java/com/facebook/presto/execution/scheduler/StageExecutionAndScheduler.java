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

import static java.util.Objects.requireNonNull;

public class StageExecutionAndScheduler
{
    private final SqlStageExecution stageExecution;
    private final StageLinkage stageLinkage;
    private final StageScheduler stageScheduler;

    StageExecutionAndScheduler(SqlStageExecution stageExecution, StageLinkage stageLinkage, StageScheduler stageScheduler)
    {
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.stageLinkage = requireNonNull(stageLinkage, "stageLinkage is null");
        this.stageScheduler = requireNonNull(stageScheduler, "stageScheduler is null");
    }

    public SqlStageExecution getStageExecution()
    {
        return stageExecution;
    }

    public StageLinkage getStageLinkage()
    {
        return stageLinkage;
    }

    public StageScheduler getStageScheduler()
    {
        return stageScheduler;
    }
}
