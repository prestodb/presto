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
import com.google.common.collect.ImmutableList;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

public class SplitRetrySourcePartitionedScheduler
        implements StageScheduler
{
    SourcePartitionedScheduler sourcePartitionedScheduler;
    SqlStageExecution stage;
    boolean isSourcePartitionedSchedulerFinished;
    int splitsScheduled;

    public SplitRetrySourcePartitionedScheduler(SourcePartitionedScheduler sourcePartitionedScheduler, SqlStageExecution stage)
    {
        this.sourcePartitionedScheduler = requireNonNull(sourcePartitionedScheduler);
        this.stage = requireNonNull(stage);
        this.splitsScheduled = 0;
    }

    @Override
    public ScheduleResult schedule()
    {
        if (isSourcePartitionedSchedulerFinished) {
            stage.transitionToSchedulingRetriedSplits();
            if (stage.getBlocked().isDone()) {
                return ScheduleResult.nonBlocked(true, ImmutableList.of(), 0, false);
            }
            else {
                return ScheduleResult.blocked(
                        false,
                        ImmutableList.of(),
                        nonCancellationPropagating(stage.getBlocked()),
                        ScheduleResult.BlockedReason.WAITING_FOR_SPLIT_RETRY,
                        0,
                        false);
            }
        }
        else {
            ScheduleResult scheduleResult = sourcePartitionedScheduler.schedule();
            sourcePartitionedScheduler.drainCompletelyScheduledLifespans();

            if (scheduleResult.isFinished()) {
                isSourcePartitionedSchedulerFinished = true;
                splitsScheduled += scheduleResult.getSplitsScheduled();

                if (!scheduleResult.isEmptySplit()) {
                    return ScheduleResult.blocked(
                            false,
                            scheduleResult.getNewTasks(),
                            nonCancellationPropagating(stage.getBlocked()),
                            ScheduleResult.BlockedReason.WAITING_FOR_SPLIT_RETRY,
                            scheduleResult.getSplitsScheduled(),
                            false);
                }
                else {
                    return ScheduleResult.nonBlocked(true, scheduleResult.getNewTasks(), scheduleResult.getSplitsScheduled(), true);
                }
            }
            else {
                splitsScheduled += scheduleResult.getSplitsScheduled();
                return scheduleResult;
            }
        }
    }

    @Override
    public void close()
    {
        sourcePartitionedScheduler.close();
    }
}
