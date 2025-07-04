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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBucketing;
import com.facebook.presto.iceberg.PartitionTransforms.ValueTransform;
import com.facebook.presto.spi.BucketFunction;
import org.apache.iceberg.PartitionSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.iceberg.PartitionTransforms.getColumnTransform;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// TODO #20578: WIP - Class implementation under development.

public class IcebergBucketFunction
        implements BucketFunction
{
    private final int bucketCount;

    private final List<PartitionColumn> partitionColumns;
//    private final List<MethodHandle> hashCodeInvokers; // TODO #20578: attribute used by Trino.
    private final List<Type> types; // TODO #20578: implementation based on HiveBucketFunction code.

    public IcebergBucketFunction(
//            TypeOperators typeOperators, // TODO #20578: attribute used by Trino.
            List<Type> types, // TODO #20578: implementation based on HiveBucketFunction code.
            PartitionSpec partitionSpec,
            List<IcebergColumnHandle> partitioningColumns,
            int bucketCount)
    {
        this.types = requireNonNull(types, "types is null"); // TODO #20578: implementation based on HiveBucketFunction code.
        requireNonNull(partitionSpec, "partitionSpec is null");
        checkArgument(!partitionSpec.isUnpartitioned(), "empty partitionSpec");
        requireNonNull(partitioningColumns, "partitioningColumns is null");
//        requireNonNull(typeOperators, "typeOperators is null");  // TODO #20578: parameter used by Trino.
        checkArgument(bucketCount > 0, "Invalid bucketCount: %s", bucketCount);

        this.bucketCount = bucketCount;

        Map<Integer, Integer> fieldIdToInputChannel = new HashMap<>();
        for (int i = 0; i < partitioningColumns.size(); i++) {
            Integer previous = fieldIdToInputChannel.put(partitioningColumns.get(i).getId(), i);
            checkState(previous == null, "Duplicate id %s in %s at %s and %s", partitioningColumns.get(i).getId(), partitioningColumns, i, previous);
        }
        partitionColumns = partitionSpec.fields().stream()
                .map(field -> {
                    Integer channel = fieldIdToInputChannel.get(field.sourceId());
                    checkArgument(channel != null, "partition field not found: %s", field);
                    Type inputType = partitioningColumns.get(channel).getType();
                    PartitionTransforms.ColumnTransform transform = getColumnTransform(field, inputType);
                    return new PartitionColumn(channel, transform.getValueTransform(), transform.getType());
                })
                .collect(toImmutableList());

        // TODO #20578: used by Trino.
//        hashCodeInvokers = partitionColumns.stream()
//                .map(PartitionColumn::getResultType)
//                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)))
//                .collect(toImmutableList());
    }

    @Override
    public int getBucket(Page page, int position)
    {
        return HiveBucketing.getBucket(bucketCount, types, page, position);

        // TODO #20578: Trino.
//        long hash = 0;
//
//        for (int i = 0; i < partitionColumns.size(); i++) {
//            PartitionColumn partitionColumn = partitionColumns.get(i);
//            Block block = page.getBlock(partitionColumn.getSourceChannel());
//            Object value = partitionColumn.getValueTransform().apply(block, position);
//            long valueHash = hashValue(hashCodeInvokers.get(i), value);
//            hash = (31 * hash) + valueHash;
//        }
//
//        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    // TODO #20578: Trino.
//    private static long hashValue(MethodHandle method, Object value)
//    {
//        if (value == null) {
//            return NULL_HASH_CODE;
//        }
//        try {
//            return (long) method.invoke(value);
//        }
//        catch (Throwable throwable) {
//            if (throwable instanceof Error) {
//                throw (Error) throwable;
//            }
//            if (throwable instanceof RuntimeException) {
//                throw (RuntimeException) throwable;
//            }
//            throw new RuntimeException(throwable);
//        }
//    }

    private static class PartitionColumn
    {
        private final int sourceChannel;
        private final ValueTransform valueTransform;
        private final Type resultType;

        public PartitionColumn(int sourceChannel, ValueTransform valueTransform, Type resultType)
        {
            this.sourceChannel = sourceChannel;
            this.valueTransform = requireNonNull(valueTransform, "valueTransform is null");
            this.resultType = requireNonNull(resultType, "resultType is null");
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }

        public Type getResultType()
        {
            return resultType;
        }

        public ValueTransform getValueTransform()
        {
            return valueTransform;
        }
    }
}
