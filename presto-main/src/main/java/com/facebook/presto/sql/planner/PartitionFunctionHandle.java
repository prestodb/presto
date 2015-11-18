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

import com.facebook.presto.operator.BucketFunction;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
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
        public BucketFunction createBucketFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");
            checkArgument(function.getBucketToPartition().get().length == 1, "Single partition can only have one partition");
            checkArgument(function.getBucketToPartition().get()[0] == 0, "Single partition binding must contain a single bucket with id 0");
            return new SingleBucketFunction();
        }
    },
    HASH {
        @Override
        public BucketFunction createBucketFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");
            int bucketCount = function.getBucketToPartition().get().length;
            if (function.getHashColumn().isPresent()) {
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
        public BucketFunction createBucketFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes)
        {
            checkArgument(function.getBucketToPartition().isPresent(), "Bucket to partition must be set before a partition function can be created");
            return new RoundRobinBucketFunction(function.getBucketToPartition().get().length);
        }
    };

    public abstract BucketFunction createBucketFunction(PartitionFunctionBinding function, List<Type> partitionChannelTypes);

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
            return MoreObjects.toStringHelper(this)
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
            return MoreObjects.toStringHelper(this)
                    .add("generator", generator)
                    .add("bucketCount", bucketCount)
                    .toString();
        }
    }
}
