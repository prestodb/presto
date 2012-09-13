package com.facebook.presto.operators;

import com.facebook.presto.BlockBuilder;
import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.ValueBlock;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class Merge
        implements BlockStream<ValueBlock>
{
    private final List<? extends BlockStream<? extends ValueBlock>> sources;
    private final TupleInfo tupleInfo;

    @SafeVarargs
    public Merge(BlockStream<? extends ValueBlock>... sources)
    {
        this(ImmutableList.copyOf(sources));
    }

    public Merge(List<? extends BlockStream<? extends ValueBlock>> sources)
    {
        // build combined tuple info
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (BlockStream<? extends ValueBlock> source : sources) {
            types.addAll(source.getTupleInfo().getTypes());
        }
        this.tupleInfo = new TupleInfo(types.build());

        this.sources = ImmutableList.copyOf(sources);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(tupleInfo, iterator());
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        return new MergeBlockIterator(this.tupleInfo, this.sources);
    }

    private static class MergeBlockIterator extends AbstractIterator<ValueBlock>
    {
        private final TupleInfo tupleInfo;
        private final List<Cursor> cursors;
        private long position;

        public MergeBlockIterator(TupleInfo tupleInfo, List<? extends BlockStream<? extends ValueBlock>> sources)
        {
            this.tupleInfo = tupleInfo;
            ImmutableList.Builder<Cursor> cursors = ImmutableList.builder();
            for (BlockStream<? extends ValueBlock> source : sources) {
                cursors.add(source.cursor());
            }
            this.cursors = cursors.build();
        }

        @Override
        protected ValueBlock computeNext()
        {
            if (!advanceCursors()) {
                endOfData();
                return null;
            }

            BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);

            // write tuple while we have room and there is more data
            do {
                for (Cursor cursor : cursors) {
                    blockBuilder.append(cursor.getTuple());
                }
            } while (!blockBuilder.isFull() && advanceCursors());

            ValueBlock block = blockBuilder.build();
            position += block.getCount();
            return block;
        }

        private boolean advanceCursors()
        {
            boolean advanced = false;
            for (Cursor cursor : cursors) {
                if (cursor.advanceNextPosition()) {
                    advanced = true;
                }
                else if (advanced) {
                    throw new IllegalStateException("Unaligned cursors");
                }
                else {
                    break;
                }
            }
            return advanced;
        }
    }
}
