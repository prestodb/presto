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
package com.facebook.presto.execution.scheduler.faulttolerant;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Memory-bounded store of {@link TaskDescriptor}s keyed by (stageId, taskPartitionId).
 * Enables fault-tolerant task retries to reconstruct identical split assignments.
 * Tracks reserved memory via {@link MemoryPool}; fails the query when capacity is exceeded.
 */
@ThreadSafe
public class TaskDescriptorStorage
{
    private static final Logger log = Logger.get(TaskDescriptorStorage.class);

    private static final String ALLOCATION_TAG = "TaskDescriptorStorage";

    private final long maxMemoryInBytes;
    private final MemoryPool memoryPool;
    private final Ticker ticker;
    private final Map<QueryId, TaskDescriptors> storages = new ConcurrentHashMap<>();

    @Inject
    public TaskDescriptorStorage(TaskDescriptorStorageConfig config, MemoryPool memoryPool)
    {
        this(config.getMaxMemory().toBytes(), memoryPool, Ticker.systemTicker());
    }

    @VisibleForTesting
    public TaskDescriptorStorage(long maxMemoryInBytes, MemoryPool memoryPool)
    {
        this(maxMemoryInBytes, memoryPool, Ticker.systemTicker());
    }

    @VisibleForTesting
    public TaskDescriptorStorage(long maxMemoryInBytes, MemoryPool memoryPool, Ticker ticker)
    {
        this.maxMemoryInBytes = maxMemoryInBytes;
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
    }

    public void initialize(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        TaskDescriptors previous = storages.putIfAbsent(queryId, new TaskDescriptors(queryId));
        checkState(previous == null, "Task descriptors storage for query %s has already been initialized", queryId);
    }

    @PreDestroy
    public void destroyAll()
    {
        for (QueryId queryId : ImmutableList.copyOf(storages.keySet())) {
            destroy(queryId);
        }
    }

    public void destroy(QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        TaskDescriptors descriptors = storages.remove(queryId);
        if (descriptors != null) {
            descriptors.destroy();
        }
    }

    public void put(TaskId taskId, TaskDescriptor descriptor)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(descriptor, "descriptor is null");
        TaskDescriptors descriptors = getTaskDescriptors(taskId);
        descriptors.put(taskId, descriptor);
    }

    public Optional<TaskDescriptor> get(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");
        TaskDescriptors descriptors = storages.get(taskId.getQueryId());
        if (descriptors == null) {
            return Optional.empty();
        }
        return descriptors.get(taskId);
    }

    public void remove(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");
        TaskDescriptors descriptors = getTaskDescriptors(taskId);
        descriptors.remove(taskId);
    }

    public long getReservedBytes()
    {
        return storages.values().stream()
                .mapToLong(TaskDescriptors::getReservedBytes)
                .sum();
    }

    private TaskDescriptors getTaskDescriptors(TaskId taskId)
    {
        TaskDescriptors descriptors = storages.get(taskId.getQueryId());
        checkState(descriptors != null, "Task descriptors storage for query %s has not been initialized", taskId.getQueryId());
        return descriptors;
    }

    @VisibleForTesting
    Optional<TaskDescriptors> getTaskDescriptors(QueryId queryId)
    {
        return Optional.ofNullable(storages.get(queryId));
    }

    @ThreadSafe
    final class TaskDescriptors
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TaskDescriptors.class).instanceSize();

        private final QueryId queryId;

        @GuardedBy("this")
        private final Map<TaskDescriptorKey, TaskDescriptorEntry> descriptors = new HashMap<>();

        @GuardedBy("this")
        private long reservedBytes;

        @GuardedBy("this")
        private boolean destroyed;

        private TaskDescriptors(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.reservedBytes = INSTANCE_SIZE;
            reserveMemory(INSTANCE_SIZE);
        }

        public synchronized void put(TaskId taskId, TaskDescriptor descriptor)
        {
            throwIfDestroyed();
            TaskDescriptorKey key = TaskDescriptorKey.from(taskId);
            TaskDescriptorEntry previous = descriptors.get(key);
            long previousBytes = previous != null ? previous.getRetainedSizeInBytes() : 0;
            long newBytes = descriptor.getRetainedSizeInBytes() + TaskDescriptorEntry.INSTANCE_SIZE;
            long delta = newBytes - previousBytes;

            if (delta > 0) {
                ensureCapacity(delta);
                reserveMemory(delta);
            }
            else if (delta < 0) {
                freeMemory(-delta);
            }

            descriptors.put(key, new TaskDescriptorEntry(descriptor, ticker.read()));
            reservedBytes += delta;

            if (delta < 0) {
                // shrink already applied above
            }
        }

        public synchronized Optional<TaskDescriptor> get(TaskId taskId)
        {
            throwIfDestroyed();
            TaskDescriptorEntry entry = descriptors.get(TaskDescriptorKey.from(taskId));
            return Optional.ofNullable(entry).map(TaskDescriptorEntry::getDescriptor);
        }

        public synchronized void remove(TaskId taskId)
        {
            throwIfDestroyed();
            TaskDescriptorEntry removed = descriptors.remove(TaskDescriptorKey.from(taskId));
            if (removed != null) {
                long bytes = removed.getRetainedSizeInBytes();
                reservedBytes -= bytes;
                freeMemory(bytes);
            }
        }

        public synchronized long getReservedBytes()
        {
            return reservedBytes;
        }

        public synchronized void destroy()
        {
            if (destroyed) {
                return;
            }
            destroyed = true;
            freeMemory(reservedBytes);
            reservedBytes = 0;
            descriptors.clear();
        }

        @GuardedBy("this")
        private void ensureCapacity(long additionalBytes)
        {
            if (getTotalReservedBytes() + additionalBytes <= maxMemoryInBytes) {
                return;
            }
            // Evict least-recently-used descriptors from other queries first, then same query older entries.
            // Without durable spill, eviction drops descriptors and may prevent retries for those tasks.
            List<QueryId> candidates = storages.entrySet().stream()
                    .filter(entry -> entry.getValue().getReservedBytes() > INSTANCE_SIZE)
                    .sorted(Comparator.comparingLong(entry -> entry.getValue().getReservedBytes()).reversed())
                    .map(Map.Entry::getKey)
                    .collect(ImmutableList.toImmutableList());

            for (QueryId candidateId : candidates) {
                if (getTotalReservedBytes() + additionalBytes <= maxMemoryInBytes) {
                    break;
                }
                if (candidateId.equals(queryId)) {
                    continue;
                }
                TaskDescriptors other = storages.get(candidateId);
                if (other != null) {
                    other.evictAll();
                    log.warn("Evicted all task descriptors for query %s to free storage capacity", candidateId);
                }
            }

            if (getTotalReservedBytes() + additionalBytes > maxMemoryInBytes) {
                evictOldestUntil(additionalBytes);
            }

            if (getTotalReservedBytes() + additionalBytes > maxMemoryInBytes) {
                throw new PrestoException(
                        EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY,
                        format(
                                "Task descriptor storage capacity exceeded for query %s: required %s bytes more, reserved %s of %s bytes",
                                queryId,
                                additionalBytes,
                                getTotalReservedBytes(),
                                maxMemoryInBytes));
            }
        }

        @GuardedBy("this")
        private void evictOldestUntil(long additionalBytes)
        {
            ImmutableList<Map.Entry<TaskDescriptorKey, TaskDescriptorEntry>> ordered = descriptors.entrySet().stream()
                    .sorted(Comparator.comparingLong(entry -> entry.getValue().getLastAccessNanos()))
                    .collect(ImmutableList.toImmutableList());

            for (Map.Entry<TaskDescriptorKey, TaskDescriptorEntry> entry : ordered) {
                if (getTotalReservedBytes() + additionalBytes <= maxMemoryInBytes) {
                    break;
                }
                TaskDescriptorEntry removed = descriptors.remove(entry.getKey());
                if (removed != null) {
                    long bytes = removed.getRetainedSizeInBytes();
                    reservedBytes -= bytes;
                    freeMemory(bytes);
                    log.warn("Evicted task descriptor %s for query %s to free storage capacity", entry.getKey(), queryId);
                }
            }
        }

        private synchronized void evictAll()
        {
            if (destroyed || descriptors.isEmpty()) {
                return;
            }
            long bytes = reservedBytes - INSTANCE_SIZE;
            descriptors.clear();
            reservedBytes = INSTANCE_SIZE;
            if (bytes > 0) {
                freeMemory(bytes);
            }
        }

        private long getTotalReservedBytes()
        {
            return TaskDescriptorStorage.this.getReservedBytes();
        }

        private void reserveMemory(long bytes)
        {
            if (bytes <= 0) {
                return;
            }
            if (!memoryPool.tryReserve(queryId, ALLOCATION_TAG, bytes)) {
                throw new PrestoException(
                        EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY,
                        format(
                                "Failed to reserve %s bytes in memory pool for task descriptor storage of query %s",
                                bytes,
                                queryId));
            }
        }

        private void freeMemory(long bytes)
        {
            if (bytes <= 0) {
                return;
            }
            memoryPool.free(queryId, ALLOCATION_TAG, bytes);
        }

        @GuardedBy("this")
        private void throwIfDestroyed()
        {
            checkState(!destroyed, "Task descriptors storage for query %s has been destroyed", queryId);
        }
    }

    private static final class TaskDescriptorKey
    {
        private final StageId stageId;
        private final int taskPartitionId;

        private TaskDescriptorKey(StageId stageId, int taskPartitionId)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.taskPartitionId = taskPartitionId;
        }

        public static TaskDescriptorKey from(TaskId taskId)
        {
            return new TaskDescriptorKey(taskId.getStageId(), taskId.getId());
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
            TaskDescriptorKey that = (TaskDescriptorKey) o;
            return taskPartitionId == that.taskPartitionId && stageId.equals(that.stageId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stageId, taskPartitionId);
        }

        @Override
        public String toString()
        {
            return stageId + "." + taskPartitionId;
        }
    }

    private static final class TaskDescriptorEntry
    {
        static final int INSTANCE_SIZE = ClassLayout.parseClass(TaskDescriptorEntry.class).instanceSize();

        private final TaskDescriptor descriptor;
        private final long lastAccessNanos;

        private TaskDescriptorEntry(TaskDescriptor descriptor, long lastAccessNanos)
        {
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.lastAccessNanos = lastAccessNanos;
        }

        public TaskDescriptor getDescriptor()
        {
            return descriptor;
        }

        public long getLastAccessNanos()
        {
            return lastAccessNanos;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + descriptor.getRetainedSizeInBytes();
        }
    }
}
