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
package com.facebook.presto.server;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.DynamicFilterSummary;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DynamicFilterService
{
    private final Map<SourceDescriptor, DynamicFilterSummaryWithSenders> dynamicFilterSummaries = new HashMap<>();
    private final Map<SourceDescriptor, SettableFuture<DynamicFilterSummary>> futures = new ConcurrentHashMap<>();
    private final Map<Integer, SourceDescriptor> autoGraphs = new ConcurrentHashMap<>();

    public void storeOrMergeSummary(String queryId, String source, int stageId, int taskId, int driverId, DynamicFilterSummary dynamicFilterSummary, int expectedSummariesCount)
    {
        DynamicFilterSummary mergedSummary;
        synchronized (this) {
            DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders = dynamicFilterSummaries.get(SourceDescriptor.of(queryId, source));
            checkState(dynamicFilterSummaryWithSenders != null, "Cannot store summary for not pre-registered task");

            mergedSummary = dynamicFilterSummaryWithSenders.addSummary(DynamicFilterSummaryWithSenders.StageTaskKey.of(stageId, taskId), driverId, dynamicFilterSummary, expectedSummariesCount);
        }

        if (mergedSummary != null) {
            DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders = dynamicFilterSummaries.get(SourceDescriptor.of(queryId, source));
            if (dynamicFilterSummaryWithSenders != null) {
                final Optional<DynamicFilterSummary> summaryIfReady = dynamicFilterSummaryWithSenders.getSummaryIfReady();
                if (summaryIfReady.isPresent()) {
                    try {
                        futures.get(SourceDescriptor.of(queryId, source)).set(summaryIfReady.get());
                    }
                    catch (Exception e) {
                        // TODO
                    }
                }
            }
        }
    }

    public synchronized void registerTasks(String source, Set<TaskId> taskIds)
    {
        if (taskIds.isEmpty()) {
            return;
        }

        String queryId = taskIds.iterator().next().getQueryId().getId();
        checkArgument(taskIds.stream().allMatch(taskId -> taskId.getQueryId().getId().equals(queryId)), "All tasks have to belong to the same query");

        checkState(!dynamicFilterSummaries.containsKey(SourceDescriptor.of(queryId, source)), "Tasks already registered");
        dynamicFilterSummaries.put(SourceDescriptor.of(queryId, source), new DynamicFilterSummaryWithSenders(taskIds.stream().map(DynamicFilterSummaryWithSenders.StageTaskKey::of).collect(toImmutableSet())));
    }

    public synchronized ListenableFuture<DynamicFilterSummary> getSummary(String queryId, String source)
    {
        SettableFuture<DynamicFilterSummary> future = futures.get(SourceDescriptor.of(queryId, source));
        if (future == null) {
            future = SettableFuture.create();
            futures.put(SourceDescriptor.of(queryId, source), future);
        }
        DynamicFilterSummaryWithSenders dynamicFilterSummaryWithSenders = dynamicFilterSummaries.get(SourceDescriptor.of(queryId, source));
        if (dynamicFilterSummaryWithSenders != null) {
            final Optional<DynamicFilterSummary> summaryIfReady
                    = dynamicFilterSummaryWithSenders.getSummaryIfReady();
            if (summaryIfReady.isPresent()) {
                future.set(summaryIfReady.get());
            }
        }
        return future;
    }

    public synchronized void removeQuery(String queryId)
    {
        dynamicFilterSummaries.entrySet().removeIf(entry -> entry.getKey().getQueryId().equals(queryId));
        futures.entrySet().removeIf(entry -> entry.getKey().getQueryId().equals(queryId));
        autoGraphs.entrySet().removeIf(entry -> entry.getValue().getQueryId().equals(queryId));
    }

    public synchronized void putAutoGraph(String queryId, String sourceId)
    {
        long autoGraph = Long.parseLong(queryId.split("_")[1] + queryId.split("_")[2] + sourceId);
        autoGraphs.put((int) (autoGraph % 10007), new SourceDescriptor(queryId, sourceId));
    }

    public synchronized boolean testAutoGraph(String queryId, String sourceId)
    {
        long autoGraph = Long.parseLong(queryId.split("_")[1] + queryId.split("_")[2] + sourceId);
        if (autoGraphs.containsKey((int) (autoGraph % 10007))) {
            return !autoGraphs.get((int) (autoGraph % 10007)).equals(new SourceDescriptor(queryId, sourceId));
        }
        else {
            return false;
        }
    }

    @Immutable
    private static class SourceDescriptor
    {
        private final String queryId;
        private final String source;

        public static SourceDescriptor of(String queryId, String source)
        {
            return new SourceDescriptor(queryId, source);
        }

        private SourceDescriptor(String queryId, String source)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.source = requireNonNull(source, "source is null");
        }

        public String getQueryId()
        {
            return queryId;
        }

        public String getSource()
        {
            return source;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }
            if (other == null || !(other instanceof SourceDescriptor)) {
                return false;
            }

            SourceDescriptor sourceDescriptor = (SourceDescriptor) other;

            return Objects.equals(queryId, sourceDescriptor.queryId) &&
                    Objects.equals(source, sourceDescriptor.source);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, source);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .addValue(queryId)
                    .addValue(source)
                    .toString();
        }
    }

    @NotThreadSafe
    static class DynamicFilterSummaryWithSenders
    {
        private final Map<StageTaskKey, SenderStats> senderStats = new HashMap<>();
        private final Set<StageTaskKey> registeredTasks;
        private DynamicFilterSummary dynamicFilterSummary;

        DynamicFilterSummaryWithSenders(Set<StageTaskKey> tasks)
        {
            this.registeredTasks = ImmutableSet.copyOf(tasks);
            for (StageTaskKey key : tasks) {
                senderStats.put(key, new SenderStats());
            }
        }

        public Optional<DynamicFilterSummary> getSummaryIfReady()
        {
            if (!senderStats.keySet().equals(registeredTasks)) {
                return Optional.empty();
            }

            for (Map.Entry<StageTaskKey, SenderStats> entry : senderStats.entrySet()) {
                if (!entry.getValue().isCompleted()) {
                    return Optional.empty();
                }
            }

            return Optional.of(dynamicFilterSummary);
        }

        public DynamicFilterSummary addSummary(StageTaskKey stageTaskId, int driverId, DynamicFilterSummary summary, int expectedSummariesCount)
        {
            SenderStats stats = senderStats.get(stageTaskId);
            checkArgument(stats != null, "Cannot add summary to not pre-registered task");
            stats.addSummary(driverId, expectedSummariesCount);

            if (dynamicFilterSummary == null) {
                dynamicFilterSummary = summary;
            }
            else {
                dynamicFilterSummary = dynamicFilterSummary.mergeWith(summary);
            }

            if (stats.isCompleted()) {
                return dynamicFilterSummary;
            }

            return null;
        }

        @NotThreadSafe
        private static class SenderStats
        {
            // collection of driverIDs reported
            private final Set<Integer> driverIds = new HashSet<>();
            private int expectedSummariesCount = -1;

            public int getExpectedSummariesCount()
            {
                return expectedSummariesCount;
            }

            public boolean isCompleted()
            {
                return (expectedSummariesCount == -1) ? false : driverIds.size() == expectedSummariesCount;
            }

            public boolean isCompleted(int expectedSummariesCount)
            {
                return driverIds.size() == expectedSummariesCount;
            }

            public void addSummary(Integer driverId, int expectedSummariesCount)
            {
                if (driverIds.contains(driverId)) {
                    // skip existing driver IDs in case of HTTP retries
                    return;
                }

                checkState(this.getExpectedSummariesCount() == -1 || this.getExpectedSummariesCount() == expectedSummariesCount, "expected summaries count should not change between summaries");

                if (this.expectedSummariesCount == -1) {
                    this.expectedSummariesCount = expectedSummariesCount;
                }
                checkState(driverIds.size() < expectedSummariesCount, "cannot increase number of received summaries beyond the expected count");

                driverIds.add(driverId);
            }
        }

        @Immutable
        private static class StageTaskKey
        {
            private final int stageId;
            private final int taskId;

            static StageTaskKey of(TaskId taskId)
            {
                return of(taskId.getStageExecutionId().getId(), taskId.getId());
            }

            static StageTaskKey of(int stageId, int taskId)
            {
                return new StageTaskKey(stageId, taskId);
            }

            private StageTaskKey(int stageId, int taskId)
            {
                this.stageId = stageId;
                this.taskId = taskId;
            }

            public int getStageId()
            {
                return stageId;
            }

            public int getTaskId()
            {
                return taskId;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                StageTaskKey that = (StageTaskKey) o;

                return Objects.equals(stageId, that.stageId) &&
                        Objects.equals(taskId, that.taskId);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(stageId, taskId);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .addValue(stageId)
                        .addValue(taskId)
                        .toString();
            }
        }
    }
}
