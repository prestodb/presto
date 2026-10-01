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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.util.SizeOf.estimatedSizeOf;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TaskDescriptor
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TaskDescriptor.class).instanceSize();

    private final int stagePartitionId;
    private final Map<PlanNodeId, List<Split>> splits;
    private final NodeRequirements nodeRequirements;
    private final long retainedSizeInBytes;

    public TaskDescriptor(
            int stagePartitionId,
            Map<PlanNodeId, List<Split>> splits,
            NodeRequirements nodeRequirements)
    {
        this.stagePartitionId = stagePartitionId;
        this.splits = ImmutableMap.copyOf(requireNonNull(splits, "splits is null"));
        this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
        this.retainedSizeInBytes = INSTANCE_SIZE
                + estimatedSizeOf(this.splits, planNodeId -> estimatedSizeOf(planNodeId.toString()), splitList -> estimatedSizeOf(splitList, Split::getRetainedSizeInBytes))
                + nodeRequirements.getRetainedSizeInBytes();
    }

    public int getStagePartitionId()
    {
        return stagePartitionId;
    }

    public Map<PlanNodeId, List<Split>> getSplits()
    {
        return splits;
    }

    public NodeRequirements getNodeRequirements()
    {
        return nodeRequirements;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
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
        TaskDescriptor that = (TaskDescriptor) o;
        return stagePartitionId == that.stagePartitionId
                && Objects.equals(splits, that.splits)
                && Objects.equals(nodeRequirements, that.nodeRequirements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stagePartitionId, splits, nodeRequirements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stagePartitionId", stagePartitionId)
                .add("splits", splits)
                .add("nodeRequirements", nodeRequirements)
                .toString();
    }
}
