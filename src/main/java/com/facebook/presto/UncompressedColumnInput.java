package com.facebook.presto;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedColumnInput
        implements Iterable<UncompressedValueBlock>
{
    private Slice slice;
    private final int valueLength;

    public UncompressedColumnInput(Slice slice, int valueLength)
    {
        this.slice = checkNotNull(slice, "slice");
        this.valueLength = valueLength;
    }

    @Override
    public Iterator<UncompressedValueBlock> iterator()
    {
        return new ColumnIterator(slice, valueLength);
    }

    private static class ColumnIterator
            extends AbstractIterator<UncompressedValueBlock>
    {
        private final Slice slice;
        private final int valueLength;
        private int position = 0;

        public ColumnIterator(Slice slice, int valueLength)
        {
            this.slice = slice;
            this.valueLength = valueLength;
            checkArgument((slice.length() % valueLength) == 0, "data must be a multiple of value length");
            checkArgument(valueLength == 8, "types other than long are not currently supported");
        }

        @Override
        protected UncompressedValueBlock computeNext()
        {
            if (position == slice.length()) {
                endOfData();
                return null;
            }

            TupleInfo tupleInfo = new TupleInfo(valueLength);
            BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);

            do {
                blockBuilder.append(slice.getLong(position));
                position += valueLength;
            }
            while ((position < slice.length()) && !blockBuilder.isFull());

            return blockBuilder.build();
        }
    }
}
