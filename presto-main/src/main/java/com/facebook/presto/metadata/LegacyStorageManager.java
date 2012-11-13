package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;

public interface LegacyStorageManager
{
    BlockIterable getBlocks(String databaseName, String tableName, int fieldIndex);
}
