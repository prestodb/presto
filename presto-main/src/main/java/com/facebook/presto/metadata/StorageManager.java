package com.facebook.presto.metadata;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.nblock.Blocks;

import java.io.IOException;

public interface StorageManager
{
    long importTableShard(TupleStream sourceTupleStream, String databaseName, String tableName)
            throws IOException;

    TupleStream getTupleStream(String databaseName, String tableName, int fieldIndex);

    Blocks getBlocks(String databaseName, String tableName, int fieldIndex);
}
