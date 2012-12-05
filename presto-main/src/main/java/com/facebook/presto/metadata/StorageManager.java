package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Operator;

import java.io.IOException;
import java.util.List;

public interface StorageManager
{
    void importShard(long shardId, List<Long> columnIds, Operator source)
            throws IOException;

    BlockIterable getBlocks(long shardId, long columnId);

    boolean shardExists(long shardId);

    void dropShard(long shardId)
        throws IOException;
}
