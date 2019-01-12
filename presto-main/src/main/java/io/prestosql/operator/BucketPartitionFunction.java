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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.BucketFunction;

import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

public class BucketPartitionFunction
        implements PartitionFunction
{
    private final BucketFunction bucketFunction;
    private final int[] bucketToPartition;
    private final int partitionCount;

    public BucketPartitionFunction(BucketFunction bucketFunction, int[] bucketToPartition)
    {
        this.bucketFunction = requireNonNull(bucketFunction, "bucketFunction is null");
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null").clone();
        partitionCount = IntStream.of(bucketToPartition).max().getAsInt() + 1;
    }

    public int getPartitionCount()
    {
        return partitionCount;
    }

    /**
     * @param functionArguments the arguments to bucketing function in order (no extra columns)
     */
    public int getPartition(Page functionArguments, int position)
    {
        int bucket = bucketFunction.getBucket(functionArguments, position);
        return bucketToPartition[bucket];
    }
}
