package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public class MergeOperator
        implements TupleStream, Iterable<UncompressedBlock>
{
    private final List<? extends TupleStream> sources;
    private final TupleInfo tupleInfo;

    public MergeOperator(TupleStream... sources)
    {
        this(ImmutableList.copyOf(sources));
    }

    public MergeOperator(Iterable<? extends TupleStream> sources)
    {
        // build combined tuple info
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TupleStream source : sources) {
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
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new GenericCursor(tupleInfo, iterator());
    }

    @Override
    public Iterator<UncompressedBlock> iterator()
    {
        return new MergeBlockIterator(this.tupleInfo, this.sources);
    }

    private static class MergeBlockIterator extends AbstractIterator<UncompressedBlock>
    {
        private final TupleInfo tupleInfo;
        private final List<Cursor> cursors;
        private long position;

        public MergeBlockIterator(TupleInfo tupleInfo, Iterable<? extends TupleStream> sources)
        {
            this.tupleInfo = tupleInfo;
            ImmutableList.Builder<Cursor> cursors = ImmutableList.builder();
            for (TupleStream source : sources) {
                cursors.add(source.cursor());
            }
            this.cursors = cursors.build();
        }

        @Override
        protected UncompressedBlock computeNext()
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

            UncompressedBlock block = blockBuilder.build();
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
