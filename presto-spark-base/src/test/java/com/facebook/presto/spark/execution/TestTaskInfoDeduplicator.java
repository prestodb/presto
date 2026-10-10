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
import com.facebook.presto.execution.TaskState;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.execution.TaskState.RUNNING;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.Decision.DUPLICATE_FINISHED;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.Decision.FOLD;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.Decision.PENDING;
import static com.facebook.presto.spark.execution.TaskInfoDeduplicator.Decision.SUPERSEDED;
import static com.facebook.presto.spark.execution.TestingTaskInfos.createTaskInfo;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTaskInfoDeduplicator
{
    private static final long SEED = 20261007;
    private static final int SEQUENCE_COUNT = 50;

    @Test
    public void testDecisions()
    {
        TaskInfoDeduplicator deduplicator = new TaskInfoDeduplicator();
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 0, FAILED, "a"), new byte[10]), PENDING);
        // ties keep the earlier attempt
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 0, ABORTED, "b"), new byte[20]), SUPERSEDED);
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 1, RUNNING, "c"), new byte[30]), PENDING);
        assertEquals(deduplicator.getPendingBytes(), 30);
        // a killed attempt that reports FINISHED wins over a higher attempt number
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 0, FINISHED, "d"), new byte[40]), FOLD);
        assertEquals(deduplicator.getPendingBytes(), 0);
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 2, FINISHED, "e"), new byte[50]), DUPLICATE_FINISHED);
        assertEquals(deduplicator.offer(createTaskInfo(1, 0, 3, FAILED, "f"), new byte[60]), SUPERSEDED);
        // attempt numbers are per fragment and partition
        assertEquals(deduplicator.offer(createTaskInfo(1, 1, 0, FAILED, "g"), new byte[70]), PENDING);
        assertEquals(deduplicator.offer(createTaskInfo(2, 0, 0, FAILED, "h"), new byte[80]), PENDING);
        assertEquals(deduplicator.getPendingBytes(), 150);

        assertEquals(deduplicator.getDuplicateFinishedCount(), 1);
        assertEquals(deduplicator.getSupersededCount(), 4);
        assertEquals(deduplicator.removePendingAttempts().size(), 2);
        assertEquals(deduplicator.getPendingBytes(), 0);
        assertTrue(deduplicator.removePendingAttempts().isEmpty());
    }

    @Test
    public void testMatchesUpdateTaskInfoMap()
    {
        Logger legacyLogger = Logger.getLogger(AbstractPrestoSparkQueryExecution.class.getName());
        Level legacyLevel = legacyLogger.getLevel();
        // updateTaskInfoMap warns about every duplicate attempt
        legacyLogger.setLevel(Level.OFF);
        try {
            Random random = new Random(SEED);
            for (int sequence = 0; sequence < SEQUENCE_COUNT; sequence++) {
                List<TaskInfo> taskInfos = createRandomSequence(random);
                assertEquals(selectWithDeduplicator(taskInfos), selectWithUpdateTaskInfoMap(taskInfos), "sequence " + sequence);
            }
        }
        finally {
            legacyLogger.setLevel(legacyLevel);
        }
    }

    static List<TaskInfo> createRandomSequence(Random random)
    {
        List<TaskInfo> taskInfos = new ArrayList<>();
        int fragmentCount = 1 + random.nextInt(3);
        for (int fragment = 0; fragment < fragmentCount; fragment++) {
            int fragmentId = 2 * fragment + random.nextInt(2);
            int partitionCount = 1 + random.nextInt(4);
            for (int partition = 0; partition < partitionCount; partition++) {
                // attempt numbers restart when Spark re-attempts a stage
                int stageAttemptCount = 1 + random.nextInt(3);
                for (int stageAttempt = 0; stageAttempt < stageAttemptCount; stageAttempt++) {
                    int taskAttemptCount = 1 + random.nextInt(3);
                    for (int attempt = 0; attempt < taskAttemptCount; attempt++) {
                        taskInfos.add(createTaskInfo(fragmentId, partition, attempt, randomState(random), "marker-" + taskInfos.size()));
                        if (random.nextInt(8) == 0) {
                            // another report for the same attempt, such as a killed attempt reporting FINISHED
                            TaskState state = random.nextBoolean() ? FINISHED : randomState(random);
                            taskInfos.add(createTaskInfo(fragmentId, partition, attempt, state, "marker-" + taskInfos.size()));
                        }
                    }
                }
            }
        }
        Collections.shuffle(taskInfos, random);
        return taskInfos;
    }

    private static TaskState randomState(Random random)
    {
        if (random.nextInt(3) == 0) {
            return FINISHED;
        }
        TaskState[] states = TaskState.values();
        return states[random.nextInt(states.length)];
    }

    private static Map<String, String> selectWithDeduplicator(List<TaskInfo> taskInfos)
    {
        TaskInfoDeduplicator deduplicator = new TaskInfoDeduplicator();
        Map<String, String> selected = new HashMap<>();
        long foldedCount = 0;
        for (int i = 0; i < taskInfos.size(); i++) {
            TaskInfo taskInfo = taskInfos.get(i);
            if (deduplicator.offer(taskInfo, Ints.toByteArray(i)) == FOLD) {
                assertNull(selected.put(getTaskKey(taskInfo), taskInfo.getNodeId()));
                foldedCount++;
            }
        }

        List<TaskInfo> pendingAttempts = new ArrayList<>();
        for (byte[] pendingAttempt : deduplicator.removePendingAttempts()) {
            TaskInfo taskInfo = taskInfos.get(Ints.fromByteArray(pendingAttempt));
            assertNotEquals(taskInfo.getTaskStatus().getState(), FINISHED);
            assertNull(selected.put(getTaskKey(taskInfo), taskInfo.getNodeId()));
            pendingAttempts.add(taskInfo);
        }
        List<TaskInfo> sortedPendingAttempts = new ArrayList<>(pendingAttempts);
        sortedPendingAttempts.sort(Comparator.comparingInt(TaskInfoDeduplicator::getFragmentId).thenComparingInt(taskInfo -> taskInfo.getTaskId().getId()));
        assertEquals(pendingAttempts, sortedPendingAttempts);

        assertEquals(deduplicator.getPendingBytes(), 0);
        assertEquals(foldedCount + pendingAttempts.size() + deduplicator.getDuplicateFinishedCount() + deduplicator.getSupersededCount(), taskInfos.size());
        return selected;
    }

    static Map<String, String> selectWithUpdateTaskInfoMap(List<TaskInfo> taskInfos)
    {
        HashMap<String, TaskInfo> taskInfoMap = new HashMap<>();
        for (TaskInfo taskInfo : taskInfos) {
            AbstractPrestoSparkQueryExecution.updateTaskInfoMap(taskInfoMap, taskInfo);
        }
        return taskInfoMap.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getNodeId()));
    }

    // the key updateTaskInfoMap uses
    static String getTaskKey(TaskInfo taskInfo)
    {
        TaskId taskId = taskInfo.getTaskId();
        return taskId.getStageExecutionId() + "." + taskId.getId();
    }
}
