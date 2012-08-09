package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Predicates.not;

public class Merge
    extends AbstractIterator<ValueBlock>
{
    private final static int TUPLES_PER_BLOCK = 1000;

    private final List<Iterator<Object>> sources;

    public Merge(List<? extends Iterator<ValueBlock>> sources)
    {
        ImmutableList.Builder<Iterator<Object>> builder = ImmutableList.builder();

        for (Iterator<ValueBlock> source : sources) {
            builder.add(Iterators.concat(Iterators.transform(source, toIterator())));
        }

        this.sources = builder.build();
    }

    private static Function<ValueBlock, Iterator<Object>> toIterator()
    {
        return new Function<ValueBlock, Iterator<Object>>()
        {
            @Override
            public Iterator<Object> apply(ValueBlock input)
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

        List<Tuple> tuples = new ArrayList<>(TUPLES_PER_BLOCK);

        while (tuples.size() < TUPLES_PER_BLOCK && Iterables.all(sources, hasNextPredicate())) {
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (Iterator<Object> source : sources) {
                builder.add(source.next());
            }

            tuples.add(new Tuple(builder.build()));
        }

        return new UncompressedValueBlock(0, (List<?>) tuples);
    }

    private Predicate<? super Iterator<Object>> hasNextPredicate()
    {
        return new Predicate<Iterator<Object>>()
        {
            @Override
            public boolean apply(Iterator<Object> input)
            {
                return input.hasNext();
            }
        };
    }
}
