package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Predicates.not;

public class Merge
        extends AbstractIterator<ValueBlock>
{
    private final List<Iterator<Tuple>> sources;
    private final TupleInfo tupleInfo;
    private long position;

    public Merge(List<? extends Iterator<ValueBlock>> sources, TupleInfo tupleInfo)
    {
        ImmutableList.Builder<Iterator<Tuple>> builder = ImmutableList.builder();
        this.tupleInfo = tupleInfo;

        for (Iterator<ValueBlock> source : sources) {
            builder.add(Iterators.concat(Iterators.transform(source, toIterator())));
        }

        this.sources = builder.build();
    }

    private static Function<ValueBlock, Iterator<Tuple>> toIterator()
    {
        return new Function<ValueBlock, Iterator<Tuple>>()
        {
            @Override
            public Iterator<Tuple> apply(ValueBlock input)
            {
                return input.iterator();
            }
        };
    }

    @Override
    protected ValueBlock computeNext()
    {
        if (Iterables.any(sources, not(hasNextPredicate()))) {
            endOfData();
            return null;
        }

        BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);

        do {
            for (Iterator<Tuple> source : sources) {
                blockBuilder.append(source.next());
            }

            if (blockBuilder.isFull()) {
                break;
            }
        } while (Iterables.all(sources, hasNextPredicate()));

        UncompressedValueBlock block = blockBuilder.build();
        position += block.getCount();
        return block;
    }

    private Predicate<? super Iterator<Tuple>> hasNextPredicate()
    {
        return new Predicate<Iterator<Tuple>>()
        {
            @Override
            public boolean apply(Iterator<Tuple> input)
            {
                return input.hasNext();
            }
        };
    }
}
