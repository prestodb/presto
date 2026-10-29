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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveNodePartitioningProvider;
import com.facebook.presto.hive.HivePartitioningHandle;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveBucketFunction.createHiveCompatibleBucketFunction;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Assigns nodes to buckets like {@link HiveNodePartitioningProvider}, but hashes rows without reading the Hive
 * session property that selects legacy timestamp bucketing, which Iceberg sessions do not have.
 */
public class IcebergNodePartitioningProvider
        extends HiveNodePartitioningProvider
{
    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        HivePartitioningHandle handle = (HivePartitioningHandle) partitioningHandle;
        checkArgument(handle.getBucketFunctionType() == HIVE_COMPATIBLE, "Unsupported bucket function type %s", handle.getBucketFunctionType());
        // The buckets only route rows to writers within one query and are never compared with bucketed Hive files.
        return createHiveCompatibleBucketFunction(bucketCount, handle.getHiveTypes().get(), false);
    }
}
