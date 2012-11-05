package com.facebook.presto.block.dictionary;

import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.Block;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock
        implements Block
{
    private final Dictionary dictionary;
    private final Block idBlock;

    public DictionaryEncodedBlock(Dictionary dictionary, Block idBlock)
    {
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(idBlock, "block is null");
        checkArgument(idBlock.getTupleInfo().equals(TupleInfo.SINGLE_LONG), "block must contain tuples with a single long value");

        this.dictionary = dictionary;
        this.idBlock = idBlock;
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

    public Block getIdBlock()
    {
        return idBlock;
    }

    @Override
    public int getPositionCount()
    {
        return idBlock.getPositionCount();
    }

    @Override
    public Range getRange()
    {
        return idBlock.getRange();
    }

    @Override
    public Range getRawRange()
    {
        return idBlock.getRawRange();
    }

    @Override
    public Block createViewPort(Range viewPortRange)
    {
        return new DictionaryEncodedBlock(dictionary, idBlock.createViewPort(viewPortRange));
    }

    @Override
    public DictionaryEncodedBlockCursor cursor()
    {
        return new DictionaryEncodedBlockCursor(dictionary, idBlock.cursor());
    }
}
