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
package com.facebook.presto.operator;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class PipelineStatus
{
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int blockedDrivers;
    private final int queuedPartitionedDrivers;
    private final long queuedPartitionedSplitsWeight;
    private final int runningPartitionedDrivers;
    private final long runningPartitionedSplitsWeight;

    public PipelineStatus(int queuedDrivers, int runningDrivers, int blockedDrivers, int queuedPartitionedDrivers, long queuedPartitionedSplitsWeight, int runningPartitionedDrivers, long runningPartitionedSplitsWeight)
    {
        this.queuedDrivers = queuedDrivers;
        this.runningDrivers = runningDrivers;
        this.blockedDrivers = blockedDrivers;
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        this.queuedPartitionedSplitsWeight = queuedPartitionedSplitsWeight;
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        this.runningPartitionedSplitsWeight = runningPartitionedSplitsWeight;
    }

    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    public long getQueuedPartitionedSplitsWeight()
    {
        return queuedPartitionedSplitsWeight;
    }

    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    public long getRunningPartitionedSplitsWeight()
    {
        return runningPartitionedSplitsWeight;
    }
}
