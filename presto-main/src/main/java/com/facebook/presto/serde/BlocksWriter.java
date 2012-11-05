package com.facebook.presto.serde;

import com.facebook.presto.tuple.Tuple;

public interface BlocksWriter
{
    /**
     * Appends the specified tuples
     */
    BlocksWriter append(Iterable<Tuple> tuples);

    /**
     * Must be called after all blocks have been appended to complete the serialization
     */
    void finish();
}
