package com.facebook.presto;

import com.facebook.presto.operators.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryEncodedBlock implements ValueBlock
{
    private final TupleInfo tupleInfo;
    private final Slice[] dictionary;
    private final ValueBlock sourceValueBlock;

    public DictionaryEncodedBlock(TupleInfo tupleInfo,  Slice[] dictionary, ValueBlock sourceValueBlock)
    {
        checkNotNull(tupleInfo, "tupleInfo is null");
        checkNotNull(dictionary, "dictionary is null");
        checkNotNull(sourceValueBlock, "sourceValueBlock is null");
        checkArgument(tupleInfo.getFieldCount() == 1, "tupleInfo should only have one column");
        this.tupleInfo = tupleInfo;
        this.dictionary = dictionary;
        this.sourceValueBlock = sourceValueBlock;
    }

    @Override
    public Optional<PositionBlock> selectPositions(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PositionBlock toPositionBlock()
    {
        return sourceValueBlock.toPositionBlock();
    }

    @Override
    public Optional<ValueBlock> selectPairs(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ValueBlock> filter(PositionBlock positions)
    {
        return MaskedValueBlock.maskBlock(this, positions);
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(Iterators.transform(
                sourceValueBlock.pairIterator(),
                new Function<Pair, Pair>()
                {
                    @Override
                    public Pair apply(Pair input)
                    {
                        return new Pair(input.getPosition(), translateValue(input.getValue()));
                    }
                }
        ));
    }

    @Override
    public Tuple getSingleValue()
    {
        return translateValue(sourceValueBlock.getSingleValue());
    }

    @Override
    public int getCount()
    {
        return sourceValueBlock.getCount();
    }

    @Override
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return sourceValueBlock.isSingleValue();
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return sourceValueBlock.isPositionsContiguous();
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return sourceValueBlock.getPositions();
    }

    @Override
    public Range getRange()
    {
        return sourceValueBlock.getRange();
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return Iterators.transform(
                sourceValueBlock.iterator(),
                new Function<Tuple, Tuple>()
                {
                    @Override
                    public Tuple apply(Tuple input)
                    {
                        return translateValue(input);
                    }
                }
        );
    }

    @Override
    public BlockCursor blockCursor()
    {
        throw new UnsupportedOperationException();
    }

    private Tuple translateValue(Tuple tupleKey) {
        checkArgument(tupleKey.getTupleInfo().getFieldCount() == 1, "should only have one column");
        int dictionaryKey = Ints.checkedCast(tupleKey.getLong(0));
        Preconditions.checkPositionIndex(dictionaryKey, dictionary.length, "dictionaryKey does not exist");
        return new Tuple(dictionary[dictionaryKey], tupleInfo);
    }
}
