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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ThriftBucketFunction
        implements BucketFunction
{
    private int bucketCount;
    private List<Integer> bucketedByPositions;

    public ThriftBucketFunction(int bucketCount, List<Integer> bucketedByPositions)
    {
        this.bucketCount = bucketCount;
        this.bucketedByPositions = requireNonNull(bucketedByPositions, "bucketedByPositions is null");
    }

    @Override
    public int getBucket(Page page, int position)
    {
        /*
            This implementation assumes bucketing by columns with string values. Currently, bucketing by other values is not supported.

            One could support this by passing in the types of the columns that define the buckets, and checking the types
            here to get the values to hash.
         */
        List<String> bucketedByValues = bucketedByPositions.stream()
                .map(e -> page.getSingleValuePage(position).getBlock(e))
                .map(block -> block.getSlice(0, 0, block.getSliceLength(0)).toStringUtf8()).collect(Collectors.toList());

        // TODO: improve hashing method
        return bucketedByValues.hashCode() % bucketCount;
    }
}
