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
package com.facebook.presto.iceberg.partitioning;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.PartitionTransforms;
import com.facebook.presto.spi.BucketFunction;

import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class IcebergBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final List<PartitionHashCalculator> calculators;
    private final List<Type> partitionChannelTypes;

    public IcebergBucketFunction(IcebergPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        requireNonNull(partitioningHandle, "partitioningHandle is null");
        checkArgument(bucketCount > 0, "Invalid bucketCount: %s", bucketCount);

        this.bucketCount = bucketCount;
        List<IcebergPartitionFieldHandle> partitionFieldHandles = partitioningHandle.getPartitionFieldHandles();
        this.calculators = partitionFieldHandles.stream()
                .map(PartitionHashCalculator::create)
                .collect(toImmutableList());
        this.partitionChannelTypes = requireNonNull(partitionChannelTypes, "partitionChannelTypes  is null");
    }

    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        for (int i = 0; i < calculators.size(); i++) {
            PartitionHashCalculator function = calculators.get(i);
            Type type = partitionChannelTypes.get(function.sourceColumnChannel);
            long valueHash = function.computeHash(page, position, type);
            hash = (31 * hash) + valueHash;
        }

        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    public static class PartitionHashCalculator
    {
        Integer sourceColumnChannel;
        PartitionTransforms.ColumnTransform columnTransform;
        PartitionHashCalculator(Integer sourceColumnChannel,
                                PartitionTransforms.ColumnTransform columnTransform)
        {
            this.sourceColumnChannel = sourceColumnChannel;
            this.columnTransform = requireNonNull(columnTransform, "columnTransform is null");
        }

        public static PartitionHashCalculator create(IcebergPartitionFieldHandle partitionFieldHandle)
        {
            PartitionTransforms.ColumnTransform columnTransform = PartitionTransforms.getColumnTransform(
                    partitionFieldHandle.getTransformString(),
                    partitionFieldHandle.getType(),
                    partitionFieldHandle.getPartitionFieldMessage());
            return new PartitionHashCalculator(
                    partitionFieldHandle.getSourceColumnChannel(),
                    columnTransform);
        }

        public long computeHash(Page page, int position, Type type)
        {
            Block block = page.getBlock(sourceColumnChannel).getSingleValueBlock(position);
            Type nt = columnTransform.getType();
            Block nblock = columnTransform.getTransform().apply(block);
            if (nt == BOOLEAN && nblock instanceof IntArrayBlock) {
                System.out.println(123123);
            }
            return columnTransform.getType().hash(columnTransform.getTransform().apply(block), 0);
        }
    }
}
