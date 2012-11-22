package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

// TODO: please kill this when ExecutionPlanner switches to using DataStreamProviders, temporarily a hack
public class LegacyStorageManagerFacade
        implements LegacyStorageManager
{
    private final StorageManager storageManager;
    private final Metadata metadata;
    private final ShardManager shardManager;

    @Inject
    public LegacyStorageManagerFacade(StorageManager storageManager, Metadata metadata, ShardManager shardManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.shardManager = checkNotNull(shardManager, "shardM is null");
    }

    @Override
    public Operator getOperator(String databaseName, String tableName, List<Integer> fieldIndexes)
    {
        final TableMetadata table = metadata.getTable("default", databaseName, tableName);

        List<BlockIterable> blockIterables = Lists.transform(fieldIndexes, new Function<Integer, BlockIterable>()
        {
            @Override
            public BlockIterable apply(Integer fieldIndex)
            {
                ColumnMetadata column = getColumn(table, fieldIndex);
                long tableId = getTableId(table);
                long columnId = getColumnId(column);

                ImmutableList.Builder<BlockIterable> blocks = ImmutableList.builder();
                for (long shardId : shardManager.getShardNodes(tableId).keySet()) {
                    blocks.add(storageManager.getBlocks(shardId, columnId));
                }
                return BlockUtils.toBlocks(Iterables.concat(blocks.build()));
            }
        });
        return new AlignmentOperator(blockIterables);
    }

    private static ColumnMetadata getColumn(TableMetadata table, int fieldIndex)
    {
        List<ColumnMetadata> columns = table.getColumns();
        checkElementIndex(fieldIndex, columns.size(), "bad fieldIndex");
        return columns.get(fieldIndex);
    }

    private static long getTableId(TableMetadata table)
    {
        checkArgument(table.getTableHandle().isPresent(), "table handle is missing");
        TableHandle handle = table.getTableHandle().get();
        checkArgument(handle instanceof NativeTableHandle, "table handle is not native");
        return ((NativeTableHandle) handle).getTableId();
    }

    private static long getColumnId(ColumnMetadata column)
    {
        checkArgument(column.getColumnHandle().isPresent(), "column handle is missing");
        ColumnHandle handle = column.getColumnHandle().get();
        checkArgument(handle instanceof NativeColumnHandle, "column handle is not native");
        return ((NativeColumnHandle) handle).getColumnId();
    }
}
