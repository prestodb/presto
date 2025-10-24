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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.BucketPartitionFunction;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SystemPartitioningHandle
        implements ConnectorPartitioningHandle
{
    enum SystemPartitioning
    {
        SINGLE,
        FIXED,
        SOURCE,
        SCALED,
        COORDINATOR_ONLY,
        ARBITRARY
    }

    public static final PartitioningHandle SINGLE_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.SINGLE, SystemPartitionFunction.SINGLE);
    public static final PartitioningHandle COORDINATOR_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.COORDINATOR_ONLY, SystemPartitionFunction.SINGLE);
    public static final PartitioningHandle FIXED_HASH_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.HASH);
    public static final PartitioningHandle FIXED_ARBITRARY_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.ROUND_ROBIN);
    public static final PartitioningHandle FIXED_BROADCAST_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.BROADCAST);
    public static final PartitioningHandle SCALED_WRITER_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.SCALED, SystemPartitionFunction.ROUND_ROBIN);
    public static final PartitioningHandle SOURCE_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.SOURCE, SystemPartitionFunction.UNKNOWN);
    public static final PartitioningHandle ARBITRARY_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.ARBITRARY, SystemPartitionFunction.UNKNOWN);
    public static final PartitioningHandle FIXED_PASSTHROUGH_DISTRIBUTION = createSystemPartitioning(SystemPartitioning.FIXED, SystemPartitionFunction.UNKNOWN);

    private static PartitioningHandle createSystemPartitioning(SystemPartitioning partitioning, SystemPartitionFunction function)
    {
        return new PartitioningHandle(Optional.empty(), Optional.empty(), new SystemPartitioningHandle(partitioning, function));
    }

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
    public boolean isSingleNode()
    {
        return function == SystemPartitionFunction.SINGLE;
    }

    @Override
    public boolean isCoordinatorOnly()
    {
        return partitioning == SystemPartitioning.COORDINATOR_ONLY;
    }

    @Override
    public boolean isBroadcast()
    {
        return function == SystemPartitionFunction.BROADCAST;
    }

    @Override
    public boolean isArbitrary()
    {
        return function == SystemPartitionFunction.ROUND_ROBIN;
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

    public PartitionFunction getPartitionFunction(List<Type> partitionChannelTypes, boolean isHashPrecomputed, int[] bucketToPartition)
    {
        requireNonNull(partitionChannelTypes, "partitionChannelTypes is null");
        requireNonNull(bucketToPartition, "bucketToPartition is null");

        BucketFunction bucketFunction = function.createBucketFunction(partitionChannelTypes, isHashPrecomputed, bucketToPartition.length);
        return new BucketPartitionFunction(bucketFunction, bucketToPartition);
    }

    public static boolean isCompatibleSystemPartitioning(PartitioningHandle first, PartitioningHandle second)
    {
        ConnectorPartitioningHandle firstConnectorHandle = first.getConnectorHandle();
        ConnectorPartitioningHandle secondConnectorHandle = second.getConnectorHandle();
        if ((firstConnectorHandle instanceof SystemPartitioningHandle) &&
                (secondConnectorHandle instanceof SystemPartitioningHandle)) {
            return ((SystemPartitioningHandle) firstConnectorHandle).getPartitioning() ==
                    ((SystemPartitioningHandle) secondConnectorHandle).getPartitioning();
        }
        return false;
    }

    public enum SystemPartitionFunction
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
                    return new HashBucketFunction(InterpretedHashGenerator.createPositionalWithTypes(partitionChannelTypes), bucketCount);
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

        public static class RoundRobinBucketFunction
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
