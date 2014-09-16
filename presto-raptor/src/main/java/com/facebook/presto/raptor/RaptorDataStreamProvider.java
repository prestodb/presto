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
package com.facebook.presto.raptor;

import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.raptor.storage.LocalStorageManager;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final LocalStorageManager storageManager;

    @Inject
    public RaptorDataStreamProvider(LocalStorageManager storageManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        RaptorSplit raptorSplit = checkType(split, RaptorSplit.class, "split");
        checkNotNull(columns, "columns is null");

        if (columns.isEmpty()) {
            return createNoColumnsOperator(operatorContext, raptorSplit);
        }
        return createAlignmentOperator(operatorContext, columns, raptorSplit);
    }

    public Operator createNoColumnsOperator(OperatorContext operatorContext, RaptorSplit raptorSplit)
    {
        RaptorColumnHandle countColumnHandle = raptorSplit.getCountColumnHandle();
        Iterable<Block> blocks = storageManager.getBlocks(raptorSplit.getShardUuid(), countColumnHandle);
        return new NoColumnsOperator(operatorContext, Iterables.transform(blocks, new Function<Block, Integer>()
        {
            @Override
            public Integer apply(Block input)
            {
                return input.getPositionCount();
            }
        }));
    }

    public Operator createAlignmentOperator(OperatorContext operatorContext, List<ConnectorColumnHandle> columns, RaptorSplit raptorSplit)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<Iterable<Block>> channels = ImmutableList.builder();
        for (ConnectorColumnHandle column : columns) {
            RaptorColumnHandle raptorColumnHandle = checkType(column, RaptorColumnHandle.class, "column");
            types.add(raptorColumnHandle.getColumnType());
            channels.add(storageManager.getBlocks(raptorSplit.getShardUuid(), column));
        }
        return new AlignmentOperator(operatorContext, types.build(), channels.build());
    }
}
