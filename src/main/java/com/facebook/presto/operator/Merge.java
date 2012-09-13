package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class Merge
        implements BlockStream, Iterable<UncompressedValueBlock>
{
    private final List<? extends BlockStream> sources;
    private final TupleInfo tupleInfo;

    public Merge(BlockStream... sources)
    {
        this(ImmutableList.copyOf(sources));
    }

    public Merge(Iterable<? extends BlockStream> sources)
    {
        // build combined tuple info
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (BlockStream source : sources) {
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
    public Iterator<UncompressedValueBlock> iterator()
    {
        return new MergeBlockIterator(this.tupleInfo, this.sources);
    }

    private static class MergeBlockIterator extends AbstractIterator<UncompressedValueBlock>
    {
        private final TupleInfo tupleInfo;
        private final List<Cursor> cursors;
        private long position;

        public MergeBlockIterator(TupleInfo tupleInfo, Iterable<? extends BlockStream> sources)
        {
            this.tupleInfo = tupleInfo;
            ImmutableList.Builder<Cursor> cursors = ImmutableList.builder();
            for (BlockStream source : sources) {
                cursors.add(source.cursor());
            }
            this.cursors = cursors.build();
        }

        @Override
        protected UncompressedValueBlock computeNext()
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

            UncompressedValueBlock block = blockBuilder.build();
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
