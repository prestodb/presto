package com.facebook.presto.block.rle;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamDeserializer;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.block.TupleStreamWriter;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.YieldingIterators;
import com.facebook.presto.block.uncompressed.UncompressedTupleInfoSerde;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.facebook.presto.block.Cursors.advanceNextValueNoYield;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncodedSerde
        implements TupleStreamSerde
{
    @Override
    public TupleStreamSerializer createSerializer()
    {
        return new TupleStreamSerializer() {
            @Override
            public TupleStreamWriter createTupleStreamWriter(SliceOutput sliceOutput)
            {
                checkNotNull(sliceOutput, "sliceOutput is null");
                return new RunLengthEncodedTupleStreamWriter(sliceOutput);
            }
        };
    }

    @Override
    public TupleStreamDeserializer createDeserializer()
    {
        return new TupleStreamDeserializer() {
            @Override
            public TupleStream deserialize(final Range totalRange, Slice slice)
            {
                checkNotNull(slice, "slice is null");

                SliceInput input = slice.getInput();
                final TupleInfo tupleInfo = UncompressedTupleInfoSerde.deserialize(input);
                final Slice dataSlice = slice.slice(input.position(), slice.length() - input.position());

                RunLengthEncodedTupleStream runLengthEncodedTupleStream = new RunLengthEncodedTupleStream(
                        tupleInfo,
                        new Iterable<RunLengthEncodedBlock>()
                        {
                            @Override
                            public Iterator<RunLengthEncodedBlock> iterator()
                            {
                                return new RunLengthEncodedIterator(tupleInfo, dataSlice.getInput(), totalRange.getStart());
                            }
                        },
                        totalRange
                );
                // present entire stream as a single "block" since RLEs are currently very small
                return new SingletonTupleStream<>(runLengthEncodedTupleStream);
//                return runLengthEncodedTupleStream;
            }

            @Override
            public Blocks deserializeBlocks(Range totalRange, Slice slice)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public class SingletonTupleStream<T extends TupleStream>
            implements TupleStream, YieldingIterable<T>
    {
        private final T source;

        public SingletonTupleStream(T source)
        {
            this.source = source;
        }

        @Override
        public YieldingIterator<T> iterator(QuerySession session)
        {
            return YieldingIterators.yieldingIterator(source);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return source.getTupleInfo();
        }

        @Override
        public Range getRange()
        {
            return source.getRange();
        }

        @Override
        public Cursor cursor(QuerySession session)
        {
            Preconditions.checkNotNull(session, "session is null");
            return source.cursor(session);
        }
    }

    private static class RunLengthEncodedIterator
            extends AbstractIterator<RunLengthEncodedBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;
        private final long positionOffset;

        private RunLengthEncodedIterator(TupleInfo tupleInfo, SliceInput sliceInput, long positionOffset)
        {
            checkNotNull(tupleInfo, "tupleInfo is null");
            checkNotNull(sliceInput, "sliceInput is null");

            this.tupleInfo = tupleInfo;
            this.sliceInput = sliceInput;
            this.positionOffset = positionOffset;
        }

        @Override
        protected RunLengthEncodedBlock computeNext()
        {
            if (sliceInput.available() == 0) {
                return endOfData();
            }
            long startPosition = sliceInput.readLong() + positionOffset;
            long endPosition = sliceInput.readLong() + positionOffset;

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

            Cursor cursor = tupleStream.cursor(new QuerySession());

            if (!initialized) {
                UncompressedTupleInfoSerde.serialize(tupleStream.getTupleInfo(), sliceOutput);
                initialized = true;
            }

            while (advanceNextValueNoYield(cursor)) {
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
