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

import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSelector;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SystemPartitioningHandle
        implements ConnectorPartitioningHandle
{
    public static Partitioning singlePartition()
    {
        return createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.SINGLE, OptionalInt.of(1), ImmutableList.of());
    }

    public static boolean isSinglePartitioning(Partitioning partitioning)
    {
        return isSystemPartitioning(partitioning, SystemPartitionFunction.SINGLE);
    }

    public static boolean isSinglePartitioning(PartitioningHandle partitioningHandle)
    {
        return isSystemPartitioning(partitioningHandle.getConnectorHandle(), SystemPartitionFunction.SINGLE);
    }

    public static Partitioning coordinatorOnlyPartition()
    {
        return createSystemPartitioning(SystemPartitioning.COORDINATOR_ONLY, SystemPartitionFunction.SINGLE, OptionalInt.of(1), ImmutableList.of());
    }

    public static Partitioning fixedHashPartitioning(int partitionCount, List<Symbol> columns)
    {
        return createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.HASH, OptionalInt.of(partitionCount), columns);
    }

    public static boolean isFixedHashPartitioning(Partitioning partitioning)
    {
        return isSystemPartitioning(partitioning, SystemPartitionFunction.HASH);
    }

    public static Partitioning fixedRandomPartitioning(int partitionCount)
    {
        return createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.ROUND_ROBIN, OptionalInt.of(partitionCount));
    }

    public static boolean isFixedRandomPartitioning(Partitioning partitioning)
    {
        return isSystemPartitioning(partitioning, SystemPartitionFunction.ROUND_ROBIN);
    }

    public static Partitioning fixedBroadcastPartitioning(int partitionCount)
    {
        return createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.BROADCAST, OptionalInt.of(partitionCount));
    }

    public static boolean isFixedBroadcastPartitioning(Partitioning partitioning)
    {
        return isSystemPartitioning(partitioning, SystemPartitionFunction.BROADCAST);
    }

    public static boolean isFixedBroadcastPartitioning(PartitioningHandle partitioningHandle)
    {
        return isSystemPartitioning(partitioningHandle.getConnectorHandle(), SystemPartitionFunction.BROADCAST);
    }

    public static Partitioning unknownPartitioning()
    {
        return createSystemPartitioning(SystemPartitioning.UNKNOWN, SystemPartitionFunction.UNKNOWN, OptionalInt.empty());
    }

    public static Partitioning unknownPartitioning(List<Symbol> columns)
    {
        return createSystemPartitioning(SystemPartitioning.UNKNOWN, SystemPartitionFunction.UNKNOWN, OptionalInt.empty(), columns);
    }

    public static boolean isUnknownPartitioning(PartitioningHandle partitioningHandle)
    {
        return isSystemPartitioning(partitioningHandle.getConnectorHandle(), SystemPartitionFunction.UNKNOWN);
    }

    private static boolean isSystemPartitioning(Partitioning partitioning, SystemPartitionFunction systemPartitionFunction)
    {
        return isSystemPartitioning(partitioning.getHandle().getConnectorHandle(), systemPartitionFunction);
    }

    private static boolean isSystemPartitioning(ConnectorPartitioningHandle connectorHandle, SystemPartitionFunction systemPartitionFunction)
    {
        if (connectorHandle instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) connectorHandle).getFunction() == systemPartitionFunction;
        }
        return false;
    }

    public static OptionalInt getSystemPartitionCount(Partitioning partitioning)
    {
        ConnectorPartitioningHandle connectorHandle = partitioning.getHandle().getConnectorHandle();
        if (connectorHandle instanceof SystemPartitioningHandle) {
            return ((SystemPartitioningHandle) connectorHandle).getPartitionCount();
        }
        return OptionalInt.empty();
    }

    private enum SystemPartitioning
    {
        FIXED,
        UNKNOWN,
        COORDINATOR_ONLY
    }

    private static Partitioning createSystemPartitioning(SystemPartitioning partitioning, SystemPartitionFunction function, OptionalInt partitionCount)
    {
        return createSystemPartitioning(partitioning, function, partitionCount, ImmutableList.of());
    }

    private static Partitioning createSystemPartitioning(SystemPartitioning partitioning, SystemPartitionFunction function, OptionalInt partitionCount, List<Symbol> columns)
    {
        return Partitioning.create(new PartitioningHandle(Optional.empty(), Optional.empty(), new SystemPartitioningHandle(partitioning, function, partitionCount)), columns);
    }

    private final SystemPartitioning partitioning;
    private final SystemPartitionFunction function;
    private final OptionalInt partitionCount;

    @JsonCreator
    public SystemPartitioningHandle(
            @JsonProperty("partitioning") SystemPartitioning partitioning,
            @JsonProperty("function") SystemPartitionFunction function,
            @JsonProperty("partitionCount") OptionalInt partitionCount)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.function = requireNonNull(function, "function is null");
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
        checkArgument(!partitionCount.isPresent() || partitionCount.getAsInt() > 0);
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

    @JsonProperty
    public OptionalInt getPartitionCount()
    {
        return partitionCount;
    }

    @Override
    public boolean isSingleNode()
    {
        return partitionCount.isPresent() && partitionCount.getAsInt() == 1;
    }

    @Override
    public boolean isCoordinatorOnly()
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
            String functionName = function.toString();
            if (function != SystemPartitionFunction.SINGLE && partitionCount.isPresent()) {
                functionName += "_" + partitionCount.getAsInt();
            }
            return functionName;
        }
        return partitioning.toString();
    }

    public NodePartitionMap getNodePartitionMap(NodeScheduler nodeScheduler)
    {
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(null);
        List<Node> nodes;
        if (partitioning == SystemPartitioning.COORDINATOR_ONLY) {
            nodes = ImmutableList.of(nodeSelector.selectCurrentNode());
        }
        else if (partitioning == SystemPartitioning.FIXED) {
            nodes = nodeSelector.selectRandomNodes(partitionCount.getAsInt());
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
        return new NodePartitionMap(partitionToNode.build(), split -> {
            throw new UnsupportedOperationException("System distribution does not support source splits");
        });
    }

    public PartitionFunction getPartitionFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        requireNonNull(bucketToPartition, "bucketToPartition is null");

        BucketFunction bucketFunction = function.createBucketFunction(partitionChannelTypes, isHashPrecomputed, bucketToPartition.length);
        return new PartitionFunction(bucketFunction, bucketToPartition);
    }

    private enum SystemPartitionFunction
    {
        SINGLE {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount)
            {
                checkArgument(bucketCount == 1, "Single partition can only have one bucket");
                return new SingleBucketFunction();
            }
        },
        HASH {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount)
            {
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
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount)
            {
                return new RoundRobinBucketFunction(bucketCount);
            }
        },
        BROADCAST {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount)
            {
                throw new UnsupportedOperationException();
            }
        },
        UNKNOWN {
            @Override
            public BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount)
            {
                throw new UnsupportedOperationException();
            }
        };

        public abstract BucketFunction createBucketFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int bucketCount);

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

            private RoundRobinBucketFunction(int bucketCount)
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

            private HashBucketFunction(HashGenerator generator, int bucketCount)
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
