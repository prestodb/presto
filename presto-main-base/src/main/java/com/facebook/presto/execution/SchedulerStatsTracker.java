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
package com.facebook.presto.execution;

public interface SchedulerStatsTracker
{
    SchedulerStatsTracker NOOP = new SchedulerStatsTracker()
    {
        @Override
        public void recordTaskUpdateDeliveredTime(long nanos) {}

        @Override
        public void recordTaskUpdateSerializedCpuTime(long nanos) {}

        @Override
        public void recordTaskPlanSerializedCpuTime(long nanos) {}

        @Override
        public void recordEventLoopMethodExecutionCpuTime(long nanos) {}

        @Override
        public void recordDeliveredUpdates(int updates) {}

        @Override
        public void recordRoundTripTime(long nanos) {}

        @Override
        public void recordStartWaitForEventLoop(long nanos) {}
    };

    void recordTaskUpdateDeliveredTime(long nanos);

    /**
     * Records task update delivery using monotonic start and end timestamps.
     *
     * @param startTimeNanos start timestamp from {@link System#nanoTime()}
     * @param endTimeNanos end timestamp from {@link System#nanoTime()}
     */
    default void recordTaskUpdateDeliveredTime(long startTimeNanos, long endTimeNanos)
    {
        recordTaskUpdateDeliveredTime(endTimeNanos - startTimeNanos);
    }

    void recordDeliveredUpdates(int updates);

    void recordRoundTripTime(long nanos);

    /**
     * Records task update round-trip timing and whether the request failed.
     *
     * @param roundTripNanos duration used by the aggregate runtime metric
     * @param startTimeNanos start timestamp from {@link System#nanoTime()}
     * @param endTimeNanos end timestamp from {@link System#nanoTime()}
     * @param failed whether the request failed
     */
    default void recordRoundTripTime(long roundTripNanos, long startTimeNanos, long endTimeNanos, boolean failed)
    {
        recordRoundTripTime(roundTripNanos);
    }

    void recordStartWaitForEventLoop(long nanos);

    /**
     * Records event-loop wait time using monotonic start and end timestamps.
     *
     * @param startTimeNanos start timestamp from {@link System#nanoTime()}
     * @param endTimeNanos end timestamp from {@link System#nanoTime()}
     */
    default void recordStartWaitForEventLoop(long startTimeNanos, long endTimeNanos)
    {
        recordStartWaitForEventLoop(endTimeNanos - startTimeNanos);
    }

    void recordTaskUpdateSerializedCpuTime(long nanos);

    void recordTaskPlanSerializedCpuTime(long nanos);

    void recordEventLoopMethodExecutionCpuTime(long nanos);
}
