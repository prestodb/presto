package com.facebook.presto.metadata;

import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.noperator.Operator;

import java.io.IOException;

public interface StorageManager
{
    long importTableShard(Operator source, String databaseName, String tableName)
            throws IOException;

    Blocks getBlocks(String databaseName, String tableName, int fieldIndex);
}
