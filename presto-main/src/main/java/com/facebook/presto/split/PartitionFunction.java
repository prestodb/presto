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
package com.facebook.presto.split;

import com.facebook.presto.metadata.TablePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.split.NativeSplitManager.NativePartition;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionFunction
        implements Function<TablePartition, Partition>
{
    private final Map<String, ColumnHandle> columnHandles;
    private final Multimap<String, ? extends PartitionKey> allPartitionKeys;

    PartitionFunction(Map<String, ColumnHandle> columnHandles,
            Multimap<String, ? extends PartitionKey> allPartitionKeys)
    {
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.allPartitionKeys = checkNotNull(allPartitionKeys, "allPartitionKeys is null");
    }

    @Override
    public Partition apply(TablePartition tablePartition)
    {
        String partitionName = tablePartition.getPartitionName();

        ImmutableMap.Builder<ColumnHandle, Domain> builder = ImmutableMap.builder();
        for (PartitionKey partitionKey : allPartitionKeys.get(partitionName)) {
            ColumnHandle columnHandle = columnHandles.get(partitionKey.getName());
            checkArgument(columnHandles != null, "Invalid partition key for column %s in partition %s", partitionKey.getName(), tablePartition.getPartitionName());

            String value = partitionKey.getValue();
            switch (partitionKey.getType()) {
                case BOOLEAN:
                    if (value.length() == 0) {
                        builder.put(columnHandle, Domain.singleValue(false));
                    }
                    else {
                        builder.put(columnHandle, Domain.singleValue(Boolean.parseBoolean(value)));
                    }
                    break;
                case LONG:
                    if (value.length() == 0) {
                        builder.put(columnHandle, Domain.singleValue(0L));
                    }
                    else {
                        builder.put(columnHandle, Domain.singleValue(Long.parseLong(value)));
                    }
                    break;
                case DOUBLE:
                    if (value.length() == 0) {
                        builder.put(columnHandle, Domain.singleValue(0.0));
                    }
                    else {
                        builder.put(columnHandle, Domain.singleValue(Double.parseDouble(value)));
                    }
                    break;
                case STRING:
                    builder.put(columnHandle, Domain.singleValue(value));
                    break;
            }
        }

        return new NativePartition(tablePartition.getPartitionId(), TupleDomain.withColumnDomains(builder.build()));
    }
}
