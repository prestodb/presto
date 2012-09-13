package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.ValueBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlockStream
        implements BlockStream<DictionaryEncodedBlock>
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final BlockStream sourceBlockStream;

    public DictionaryEncodedBlockStream(TupleInfo tupleInfo, Slice[] dictionary, BlockStream sourceBlockStream)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceBlockStream, "sourceBlockStream is null");
        checkArgument(tupleInfo.getFieldCount() == 1, "tupleInfo should only have one column");

        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceBlockStream = sourceBlockStream;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Cursor cursor()
    {
        return new DictionaryEncodedCursor(tupleInfo, dictionary, sourceBlockStream.cursor());
    }

    @Override
    public Iterator<DictionaryEncodedBlock> iterator()
    {
        return Iterators.transform(
                sourceBlockStream.iterator(),
                new Function<ValueBlock, DictionaryEncodedBlock>()
                {
                    @Override
                    public DictionaryEncodedBlock apply(ValueBlock input)
                    {
                        return new DictionaryEncodedBlock(tupleInfo, dictionary, input);
                    }
                }
        );
    }
}
