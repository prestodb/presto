package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Operator;

import java.io.IOException;

public interface StorageManager
{
    long importTableShard(Operator source, String databaseName, String tableName)
            throws IOException;

    BlockIterable getBlocks(String databaseName, String tableName, int fieldIndex);
}
