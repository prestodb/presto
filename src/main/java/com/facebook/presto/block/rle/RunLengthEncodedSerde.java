package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.BlockStreamSerde;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.ValueBlock;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class RunLengthEncodedSerde implements BlockStreamSerde<RunLengthEncodedBlock>
{
    @Override
    public void serialize(BlockStream<? extends ValueBlock> blockStream, SliceOutput sliceOutput)
    {
        checkNotNull(blockStream, "blockStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");

        UncompressedTupleInfoSerde.serialize(blockStream.getTupleInfo(), sliceOutput);

        Cursor cursor = blockStream.cursor();

        if (!cursor.advanceNextValue()) {
            // Nothing more to do
            return;
        }

        long startPosition = cursor.getPosition();
        long endPosition = cursor.getCurrentValueEndPosition();
        Tuple lastTuple = cursor.getTuple();
        while (cursor.advanceNextValue()) {
            if (cursor.getPosition() != endPosition + 1 || !cursor.currentValueEquals(lastTuple)) {
                // Flush out block if next value position is not contiguous, or a different tuple
                sliceOutput.writeLong(startPosition);
                sliceOutput.writeLong(endPosition);
                sliceOutput.writeBytes(lastTuple.getTupleSlice());
                // TODO: we can further compact this if positions are mostly contiguous

                lastTuple = cursor.getTuple();
                startPosition = cursor.getPosition();
            }
            endPosition = cursor.getCurrentValueEndPosition();
        }

        // Flush out final block
        sliceOutput.writeLong(startPosition);
        sliceOutput.writeLong(endPosition);
        sliceOutput.writeBytes(lastTuple.getTupleSlice());
    }

    @Override
    public BlockStream<RunLengthEncodedBlock> deserialize(Slice slice)
    {
        checkNotNull(slice, "slice is null");

        SliceInput input = slice.input();
        final TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(input);
        final Slice dataSlice = input.slice();

        return new RunLengthEncodedBlockStream(
                tupleInfo,
                new Iterable<RunLengthEncodedBlock>()
                {
                    @Override
                    public Iterator<RunLengthEncodedBlock> iterator()
                    {
                        return new RunLengthEncodedIterator(tupleInfo, dataSlice.input());
                    }
                }
        );
    }

    private static class RunLengthEncodedIterator extends AbstractIterator<RunLengthEncodedBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;

        private RunLengthEncodedIterator(TupleInfo tupleInfo, SliceInput sliceInput)
        {
            checkNotNull(tupleInfo, "tupleInfo is null");
            checkNotNull(sliceInput, "sliceInput is null");

            this.tupleInfo = tupleInfo;
            this.sliceInput = sliceInput;
        }

        @Override
        protected RunLengthEncodedBlock computeNext()
        {
            if (sliceInput.available() == 0) {
                return endOfData();
            }
            long startPosition = sliceInput.readLong();
            long endPosition = sliceInput.readLong();
            Tuple tuple = tupleInfo.extractTuple(sliceInput);
            return new RunLengthEncodedBlock(tuple, Range.create(startPosition, endPosition));
        }
    }
}
