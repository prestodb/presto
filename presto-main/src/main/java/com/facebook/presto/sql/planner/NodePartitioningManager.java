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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodePartitioningManager
{
    private final NodeScheduler nodeScheduler;

    @Inject
    public NodePartitioningManager(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    public NodePartitionMap getNodePartitioningMap(Session session, PartitioningHandle partitioningHandle)
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");

        if (!(partitioningHandle instanceof SystemPartitioningHandle)) {
            throw new IllegalArgumentException("Unsupported partitioning handle " + partitioningHandle.getClass().getName());
        }

        return ((SystemPartitioningHandle) partitioningHandle).getNodePartitionMap(session, nodeScheduler);
    }

    public PartitionFunction getPartitionFunction(Session session, PartitionFunctionBinding functionBinding, List<Type> partitionChannelTypes)
    {
        PartitioningHandle partitioningHandle = functionBinding.getPartitioningHandle();
        if (!(partitioningHandle instanceof SystemPartitioningHandle)) {
            throw new IllegalArgumentException("Unsupported distribution handle " + partitioningHandle.getClass().getName());
        }

        checkArgument(functionBinding.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");

        return ((SystemPartitioningHandle) partitioningHandle).getPartitionFunction(
                partitionChannelTypes,
                functionBinding.getHashColumn().isPresent(),
                functionBinding.getBucketToPartition().get());
    }
}
