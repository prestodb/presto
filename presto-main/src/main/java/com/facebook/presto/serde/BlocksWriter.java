package com.facebook.presto.serde;

import com.facebook.presto.Tuple;
import com.facebook.presto.block.Block;

public interface BlocksWriter
{
    /**
     * Appends the specified tuple to this serialization
     */
    BlocksWriter append(Tuple tuple);

    /**
     * Appends the specified block to this serialization
     */
    BlocksWriter append(Block block);

    /**
     * Must be called after all blocks have been appended to complete the serialization
     */
    void finish();
}
