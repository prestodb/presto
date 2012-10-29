package com.facebook.presto.metadata;

import com.facebook.presto.block.TupleStream;

import java.io.IOException;

public interface StorageManager
{
    long importTableShard(TupleStream sourceTupleStream, String databaseName, String tableName)
            throws IOException;

    TupleStream getTupleStream(String databaseName, String tableName, int fieldIndex);

    Iterable<TupleStream> getIterable(String databaseName, String tableName, int fieldIndex);
}
