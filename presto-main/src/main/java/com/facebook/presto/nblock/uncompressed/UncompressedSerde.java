/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.nblock.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamDeserializer;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class UncompressedSerde
        implements TupleStreamSerde
{
    @Override
    public TupleStreamSerializer createSerializer()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleStreamDeserializer createDeserializer()
    {
        return new TupleStreamDeserializer() {
            @Override
            public TupleStream deserialize(Range totalRange, Slice slice)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Blocks deserializeBlocks(final Range totalRange, final Slice slice)
            {
                return UncompressedSerde.deserializeBlocks(totalRange, slice);
            }
        };
    }

    public static Blocks deserializeBlocks(final Range totalRange, final Slice slice)
    {
        return new Blocks() {
            @Override
            public TupleInfo getTupleInfo()
            {
                return new UncompressedReader(0, slice).tupleInfo;
            }

            @Override
            public Iterator<Block> iterator()
            {
                return new UncompressedReader(totalRange.getStart(), slice);
            }
        };
    }

    private static class UncompressedReader
            extends AbstractIterator<Block>
    {
        private final long positionOffset;
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;

        private UncompressedReader(long positionOffset, Slice slice)
        {
            this.positionOffset = positionOffset;
            sliceInput = slice.input();
            this.tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            int blockSize = sliceInput.readInt();
            int tupleCount = sliceInput.readInt();
            long startPosition = sliceInput.readLong() + positionOffset;

            Range range = Range.create(startPosition, startPosition + tupleCount - 1);

            Slice block = sliceInput.readSlice(blockSize);
            return new UncompressedBlock(range, tupleInfo, block);
        }
    }
}
