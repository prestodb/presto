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

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.MoreObjects;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public enum PartitionFunctionHandle
{
    SINGLE {
        @Override
        public PartitionFunction createPartitionFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getPartitionCount().isPresent(), "Partition count must be set before a partition function can be created");
            checkArgument(function.getPartitionCount().getAsInt() == 1, "Single partition can only have one partition");
            return new SinglePartitionFunction();
        }
    },
    HASH {
        @Override
        public PartitionFunction createPartitionFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getPartitionCount().isPresent(), "Partition count must be set before a partition function can be created");
            int partitionCount = function.getPartitionCount().getAsInt();
            if (function.getHashColumn().isPresent()) {
                return new HashPartitionFunction(new PrecomputedHashGenerator(0), partitionCount);
            }
            else {
                int[] hashChannels = new int[partitionChannelTypes.size()];
                for (int i = 0; i < partitionChannelTypes.size(); i++) {
                    hashChannels[i] = i;
                }

                return new HashPartitionFunction(new InterpretedHashGenerator(partitionChannelTypes, hashChannels), partitionCount);
            }
        }
    },
    ROUND_ROBIN {
        @Override
        public PartitionFunction createPartitionFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getPartitionCount().isPresent(), "Partition count must be set before a partition function can be created");
            return new RoundRobinPartitionFunction(function.getPartitionCount().getAsInt());
        }
    };

    public abstract PartitionFunction createPartitionFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes);

    private static class SinglePartitionFunction
            implements PartitionFunction
    {
        @Override
        public int getPartitionCount()
        {
            return 1;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return 0;
        }
    }

    private static class RoundRobinPartitionFunction
            implements PartitionFunction
    {
        private final int partitionCount;
        private int counter;

        public RoundRobinPartitionFunction(int partitionCount)
        {
            checkArgument(partitionCount > 0, "partitionCount must be at least 1");
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            int partition = counter % partitionCount;
            counter = (counter + 1) & 0x7fff_ffff;
            return partition;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitionCount", partitionCount)
                    .toString();
        }
    }

    private static class HashPartitionFunction
            implements PartitionFunction
    {
        private final HashGenerator generator;
        private final int partitionCount;

        public HashPartitionFunction(HashGenerator generator, int partitionCount)
        {
            checkArgument(partitionCount > 0, "partitionCount must be at least 1");
            this.generator = generator;
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return generator.getPartition(partitionCount, position, page);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("generator", generator)
                    .add("partitionCount", partitionCount)
                    .toString();
        }
    }
}
