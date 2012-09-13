package com.facebook.presto.block;

public interface ValueBlock
        extends Block
{
    BlockCursor blockCursor();
}
