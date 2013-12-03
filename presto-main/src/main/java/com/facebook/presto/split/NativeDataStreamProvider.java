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

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class NativeDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final LocalStorageManager storageManager;

    @Inject
    public NativeDataStreamProvider(LocalStorageManager storageManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof NativeSplit;
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new AlignmentOperator(operatorContext, createChannels(split, columns));
    }

    private List<BlockIterable> createChannels(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof NativeSplit, "Split must be of type NativeType, not %s", split.getClass().getName());
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        NativeSplit nativeSplit = (NativeSplit) split;

        ImmutableList.Builder<BlockIterable> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof NativeColumnHandle, "column must be native, not %s", column);
            builder.add(storageManager.getBlocks(nativeSplit.getShardUuid(), column));
        }
        return builder.build();
    }
}
