package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryOrderByOperator
        implements Operator
{
    private static final int MAX_IN_MEMORY_SORT_SIZE = 1_000_000;
    
    private final Operator source;
    private final int keyChannelIndex;
    private final List<ProjectionFunction> projections;
    private final Ordering<TupleReadable> ordering;
    private final ImmutableList<TupleInfo> tupleInfos;

    public InMemoryOrderByOperator(Operator source, int keyChannelIndex, List<ProjectionFunction> projections, Ordering<TupleReadable> ordering)
    {
        checkNotNull(source, "source is null");
        checkArgument(keyChannelIndex >= 0, "keyChannelIndex must be at least zero");
        checkNotNull(projections, "projections is null");
        checkArgument(!projections.isEmpty(), "projections is empty");
        checkNotNull(ordering, "ordering is null");
        this.source = source;
        this.keyChannelIndex = keyChannelIndex;
        this.projections = ImmutableList.copyOf(projections);
        this.ordering = ordering;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    public InMemoryOrderByOperator(Operator source, int keyChannelIndex, List<ProjectionFunction> projections)
    {
        this(source, keyChannelIndex, projections, Ordering.from(FieldOrderedTupleComparator.INSTANCE));
    }

    @Override
    public int getChannelCount()
    {
        return projections.size();
    }

    @Override
    public ImmutableList<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new InMemoryOrderByIterator(source.iterator());
    }

    private class InMemoryOrderByIterator
            extends AbstractIterator<Page>
    {
        private final Iterator<Page> pageIterator;
        private Iterator<KeyAndTuples> outputIterator;
        private long position;

        private InMemoryOrderByIterator(Iterator<Page> pageIterator)
        {
            this.pageIterator = pageIterator;
        }

        @Override
        protected Page computeNext()
        {
            if (outputIterator == null) {
                outputIterator = materializeTuplesAndSort().iterator();
            }

            if (!outputIterator.hasNext()) {
                return endOfData();
            }

            BlockBuilder[] outputs = new BlockBuilder[projections.size()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(projections.get(i).getTupleInfo());
            }

            while (!isFull(outputs) && outputIterator.hasNext()) {
                KeyAndTuples next = outputIterator.next();
                for (int i = 0; i < projections.size(); i++) {
                    projections.get(i).project(next.getTuples(), outputs[i]);
                }
            }

            Block[] blocks = new Block[projections.size()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            position += page.getPositionCount();
            return page;
        }

        private List<KeyAndTuples> materializeTuplesAndSort() {
            List<KeyAndTuples> keyAndTuplesList = Lists.newArrayList();
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                Block[] blocks = page.getBlocks();
                BlockCursor[] cursors = new BlockCursor[blocks.length];
                for (int i = 0; i < cursors.length; i++) {
                    cursors[i] = blocks[i].cursor();
                }
                for (int position = 0; position < page.getPositionCount(); position++) {
                    for (BlockCursor cursor : cursors) {
                        checkState(cursor.advanceNextPosition());
                    }
                    keyAndTuplesList.add(getKeyAndTuples(cursors));
                }
                checkState(keyAndTuplesList.size() <= MAX_IN_MEMORY_SORT_SIZE, "Too many tuples for in memory sort");
            }
            Collections.sort(keyAndTuplesList, KeyAndTuples.keyComparator(ordering));
            return keyAndTuplesList;
        }

        private KeyAndTuples getKeyAndTuples(BlockCursor[] cursors)
        {
            // TODO: pre-project columns to minimize storage in global candidate set
            Tuple key = cursors[keyChannelIndex].getTuple();
            Tuple[] tuples = new Tuple[cursors.length];
            for (int channel = 0; channel < cursors.length; channel++) {
                tuples[channel] = (channel == keyChannelIndex) ? key : cursors[channel].getTuple();
            }
            return new KeyAndTuples(key, tuples);
        }

        private boolean isFull(BlockBuilder... outputs)
        {
            for (BlockBuilder output : outputs) {
                if (output.isFull()) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class KeyAndTuples
    {
        private final Tuple key;
        private final Tuple[] tuples;

        private KeyAndTuples(Tuple key, Tuple[] tuples)
        {
            this.key = key;
            this.tuples = tuples;
        }

        public Tuple getKey()
        {
            return key;
        }

        public Tuple[] getTuples()
        {
            return tuples;
        }

        public static Comparator<KeyAndTuples> keyComparator(final Comparator<TupleReadable> tupleReadableComparator)
        {
            return new Comparator<KeyAndTuples>()
            {
                @Override
                public int compare(KeyAndTuples o1, KeyAndTuples o2)
                {
                    return tupleReadableComparator.compare(o1.getKey(), o2.getKey());
                }
            };
        }
    }
}
