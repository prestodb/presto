package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;

public interface ValueBlock
        extends Block
{
    BlockCursor blockCursor();
}
