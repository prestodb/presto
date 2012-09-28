package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.google.common.base.Preconditions.*;

public class RunLengthEncodedSerde
        implements TupleStreamSerde
{
    @Override
    public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new RunLengthEncodedTupleStreamWriter(sliceOutput);
    }

    @Override
    public TupleStream deserialize(Slice slice)
    {
        checkNotNull(slice, "slice is null");

        SliceInput input = slice.input();
        final TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(input);
        final Slice dataSlice = input.slice();

        return new RunLengthEncodedTupleStream(
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

    private static class RunLengthEncodedIterator
            extends AbstractIterator<RunLengthEncodedBlock>
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

    private static class RunLengthEncodedTupleStreamWriter
            implements TupleStreamWriter
    {
        private final SliceOutput sliceOutput;
        private boolean initialized;
        private boolean finished;

        private long startPosition = -1;
        private long endPosition = -1;
        private Tuple lastTuple;

        private RunLengthEncodedTupleStreamWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public TupleStreamWriter append(TupleStream tupleStream)
        {
            checkNotNull(tupleStream, "tupleStream is null");
            checkState(!finished, "already finished");

            Cursor cursor = tupleStream.cursor();

            if (!initialized) {
                UncompressedTupleInfoSerde.serialize(tupleStream.getTupleInfo(), sliceOutput);
                initialized = true;
            }

            while (cursor.advanceNextValue()) {
                if (lastTuple == null) {
                    startPosition = cursor.getPosition();
                    endPosition = cursor.getCurrentValueEndPosition();
                    lastTuple = cursor.getTuple();
                }
                else {
                    checkArgument(cursor.getPosition() > endPosition, "positions are not increasing");
                    if (cursor.getPosition() != endPosition + 1 || !cursor.currentTupleEquals(lastTuple)) {
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
            }

            return this;
        }

        @Override
        public void finish()
        {
            checkState(initialized, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            if (lastTuple != null) {
                // Flush out final block if there exists one (null if they were all empty blocks)
                sliceOutput.writeLong(startPosition);
                sliceOutput.writeLong(endPosition);
                sliceOutput.writeBytes(lastTuple.getTupleSlice());
            }
        }
    }
}
