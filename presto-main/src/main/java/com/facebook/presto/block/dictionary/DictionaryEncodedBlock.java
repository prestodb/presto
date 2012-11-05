package com.facebook.presto.block.dictionary;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock
        implements Block
{
    private final Dictionary dictionary;
    private final Block block;

    public DictionaryEncodedBlock(Dictionary dictionary, Block block)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(block, "block is null");
        checkArgument(block.getTupleInfo().equals(TupleInfo.SINGLE_LONG), "block must contain tuples with a single long value");

        this.dictionary = dictionary;
        this.block = block;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return dictionary.getTupleInfo();
    }

    public Dictionary getDictionary()
    {
        return dictionary;
    }

    @Override
    public int getPositionCount()
    {
        return block.getPositionCount();
    }

    @Override
    public Range getRange()
    {
        return block.getRange();
    }

    @Override
    public Range getRawRange()
    {
        return block.getRawRange();
    }

    @Override
    public Block createViewPort(Range viewPortRange)
    {
        return new DictionaryEncodedBlock(dictionary, block.createViewPort(viewPortRange));
    }

    @Override
    public DictionaryEncodedBlockCursor cursor()
    {
        return new DictionaryEncodedBlockCursor(dictionary, block.cursor());
    }
}
