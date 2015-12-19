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
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.operator.BucketFunction;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SystemPartitioningHandle
        implements PartitioningHandle
{
    private enum SystemPartitioning
    {
        SINGLE,
        FIXED,
        SOURCE,
        COORDINATOR_ONLY
    }

    public static final SystemPartitioningHandle SINGLE_DISTRIBUTION = new SystemPartitioningHandle(SystemPartitioning.SINGLE, SystemPartitionFunction.SINGLE);
    public static final SystemPartitioningHandle COORDINATOR_DISTRIBUTION = new SystemPartitioningHandle(SystemPartitioning.COORDINATOR_ONLY, SystemPartitionFunction.SINGLE);
    public static final SystemPartitioningHandle FIXED_HASH_DISTRIBUTION = new SystemPartitioningHandle(SystemPartitioning.FIXED, SystemPartitionFunction.HASH);
    public static final SystemPartitioningHandle FIXED_RANDOM_DISTRIBUTION = new SystemPartitioningHandle(SystemPartitioning.FIXED, SystemPartitionFunction.ROUND_ROBIN);
    public static final SystemPartitioningHandle SOURCE_DISTRIBUTION = new SystemPartitioningHandle(SystemPartitioning.SOURCE, SystemPartitionFunction.UNKNOWN);

    private final SystemPartitioning partitioning;
    private final SystemPartitionFunction function;

    @JsonCreator
    public SystemPartitioningHandle(
            @JsonProperty("partitioning") SystemPartitioning partitioning,
            @JsonProperty("function") SystemPartitionFunction function)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.function = requireNonNull(function, "function is null");
    }

    @JsonProperty
    public SystemPartitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public SystemPartitionFunction getFunction()
    {
        return function;
    }

    @Override
    public boolean isSingleNodeDistribution()
    {
        return partitioning == SystemPartitioning.COORDINATOR_ONLY || partitioning == SystemPartitioning.SINGLE;
    }

    @Override
    public boolean isCoordinatorOnlyDistribution()
    {
        return partitioning == SystemPartitioning.COORDINATOR_ONLY;
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
        SystemPartitioningHandle that = (SystemPartitioningHandle) o;
        return partitioning == that.partitioning &&
                function == that.function;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, function);
    }

    @Override
    public String toString()
    {
        if (partitioning == SystemPartitioning.FIXED) {
            return function.toString();
        }
        return partitioning.toString();
    }

    public NodePartitionMap getNodePartitionMap(Session session, NodeScheduler nodeScheduler)
    {
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
        List<Node> nodes;
        if (partitioning == SystemPartitioning.COORDINATOR_ONLY) {
            nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
        }
        else if (partitioning == SystemPartitioning.SINGLE) {
            nodes = nodeSelector.selectRandomNodes(1);
        }
        else if (partitioning == SystemPartitioning.FIXED) {
            nodes = nodeSelector.selectRandomNodes(getHashPartitionCount(session));
        }
        else {
            throw new IllegalArgumentException("Unsupported plan distribution " + partitioning);
        }

        checkCondition(!nodes.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");

        ImmutableMap.Builder<Integer, Node> partitionToNode = ImmutableMap.builder();
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            partitionToNode.put(i, node);
        }
        return new NodePartitionMap(partitionToNode.build());
    }

    public PartitionFunction getPartitionFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        requireNonNull(bucketToPartition, "bucketToPartition is null");

        BucketFunction bucketFunction = function.createBucketFunction(partitionChannelTypes, isHashPrecomputed, bucketToPartition);
        return new PartitionFunction(bucketFunction, bucketToPartition);
    }

    public enum SystemPartitionFunction
    {
        SINGLE {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
            {
                checkArgument(bucketToPartition.length == 1, "Single partition can only have one partition");
                checkArgument(bucketToPartition[0] == 0, "Single partition binding must contain a single bucket with id 0");
                return new SingleBucketFunction();
            }
        },
        HASH {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
            {
                int bucketCount = bucketToPartition.length;
                if (isHashPrecomputed) {
                    return new HashBucketFunction(new PrecomputedHashGenerator(0), bucketCount);
                }
                else {
                    int[] hashChannels = new int[partitionChannelTypes.size()];
                    for (int i = 0; i < partitionChannelTypes.size(); i++) {
                        hashChannels[i] = i;
                    }

                    return new HashBucketFunction(new InterpretedHashGenerator(partitionChannelTypes, hashChannels), bucketCount);
                }
            }
        },
        ROUND_ROBIN {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
            {
                return new RoundRobinBucketFunction(bucketToPartition.length);
            }
        },
        UNKNOWN {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
            {
                throw new UnsupportedOperationException();
            }
        };

        public abstract BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition);

        private static class SingleBucketFunction
                implements BucketFunction
        {
            @Override
            public int getBucket(Page page, int position)
            {
                return 0;
            }
        }

        private static class RoundRobinBucketFunction
                implements BucketFunction
        {
            private final int bucketCount;
            private int counter;

            public RoundRobinBucketFunction(int bucketCount)
            {
                checkArgument(bucketCount > 0, "bucketCount must be at least 1");
                this.bucketCount = bucketCount;
            }

            @Override
            public int getBucket(Page page, int position)
            {
                int bucket = counter % bucketCount;
                counter = (counter + 1) & 0x7fff_ffff;
                return bucket;
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("bucketCount", bucketCount)
                        .toString();
            }
        }

        private static class HashBucketFunction
                implements BucketFunction
        {
            private final HashGenerator generator;
            private final int bucketCount;

            public HashBucketFunction(HashGenerator generator, int bucketCount)
            {
                checkArgument(bucketCount > 0, "partitionCount must be at least 1");
                this.generator = generator;
                this.bucketCount = bucketCount;
            }

            @Override
            public int getBucket(Page page, int position)
            {
                return generator.getPartition(bucketCount, position, page);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("generator", generator)
                        .add("bucketCount", bucketCount)
                        .toString();
            }
        }
    }
}
