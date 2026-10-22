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
package com.facebook.presto.spark.execution;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.execution.TaskState.FINISHED;
import static java.util.Objects.requireNonNull;

/**
 * Selects the same attempt of each (fragment, partition) as {@link AbstractPrestoSparkQueryExecution#updateTaskInfoMap}
 * when offered the task infos in the same order: the first FINISHED attempt, otherwise the first attempt with the
 * highest attempt number. The selection only depends on the state reported by the task info itself, because a killed
 * attempt can report FINISHED. A FINISHED attempt can be folded as soon as it arrives, while the best other attempt
 * is kept compressed until a FINISHED attempt supersedes it or the aggregation is sealed.
 */
final class TaskInfoDeduplicator
{
    enum Decision
    {
        FOLD,
        DUPLICATE_FINISHED,
        SUPERSEDED,
        PENDING
    }

    private final Map<Integer, Fragment> fragments = new TreeMap<>();
    private long pendingBytes;
    private long duplicateFinishedCount;
    private long supersededCount;

    Decision offer(TaskInfo taskInfo, byte[] compressedTaskInfo)
    {
        TaskId taskId = taskInfo.getTaskId();
        int partition = taskId.getId();
        Fragment fragment = fragments.computeIfAbsent(getFragmentId(taskInfo), ignored -> new Fragment());

        if (taskInfo.getTaskStatus().getState() == FINISHED) {
            if (fragment.finished.get(partition)) {
                duplicateFinishedCount++;
                return Decision.DUPLICATE_FINISHED;
            }
            fragment.finished.set(partition);
            PendingAttempt superseded = fragment.pending.remove(partition);
            if (superseded != null) {
                pendingBytes -= superseded.compressedTaskInfo.length;
                supersededCount++;
            }
            return Decision.FOLD;
        }

        if (fragment.finished.get(partition)) {
            supersededCount++;
            return Decision.SUPERSEDED;
        }
        PendingAttempt current = fragment.pending.get(partition);
        if (current != null && current.attemptNumber >= taskId.getAttemptNumber()) {
            supersededCount++;
            return Decision.SUPERSEDED;
        }
        fragment.pending.put(partition, new PendingAttempt(taskId.getAttemptNumber(), compressedTaskInfo));
        pendingBytes += compressedTaskInfo.length;
        if (current != null) {
            pendingBytes -= current.compressedTaskInfo.length;
            supersededCount++;
        }
        return Decision.PENDING;
    }

    /**
     * Removes the pending attempts and returns them ordered by fragment and partition.
     */
    List<byte[]> removePendingAttempts()
    {
        List<byte[]> pendingAttempts = new ArrayList<>();
        for (Fragment fragment : fragments.values()) {
            for (PendingAttempt pendingAttempt : fragment.pending.values()) {
                pendingAttempts.add(pendingAttempt.compressedTaskInfo);
            }
            fragment.pending.clear();
        }
        pendingBytes = 0;
        return pendingAttempts;
    }

    long getPendingBytes()
    {
        return pendingBytes;
    }

    long getDuplicateFinishedCount()
    {
        return duplicateFinishedCount;
    }

    long getSupersededCount()
    {
        return supersededCount;
    }

    static int getFragmentId(TaskInfo taskInfo)
    {
        return taskInfo.getTaskId().getStageExecutionId().getStageId().getId();
    }

    private static class Fragment
    {
        private final BitSet finished = new BitSet();
        private final Map<Integer, PendingAttempt> pending = new TreeMap<>();
    }

    private static class PendingAttempt
    {
        private final int attemptNumber;
        private final byte[] compressedTaskInfo;

        private PendingAttempt(int attemptNumber, byte[] compressedTaskInfo)
        {
            this.attemptNumber = attemptNumber;
            this.compressedTaskInfo = requireNonNull(compressedTaskInfo, "compressedTaskInfo is null");
        }
    }
}
