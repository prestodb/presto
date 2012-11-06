package com.facebook.presto.serde;

import com.facebook.presto.tuple.Tuple;

public interface Encoder
{
    /**
     * Appends the specified tuples
     */
    Encoder append(Iterable<Tuple> tuples);

    /**
     * Must be called after all blocks have been appended to complete the serialization
     */
    BlockEncoding finish();
}
