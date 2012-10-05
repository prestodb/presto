package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a TupleStream and produces an ordered iteration of smaller range-contiguous TupleStreams
 * that spans the entire range of the original TupleStream.
 *
 * NOTE: In order for this to work properly, once a new "chunk" TupleStream is produced by the iterator,
 * you should not access any of the previously generated "chunk" TupleStreams from the same iterator
 * in any fashion.
 */
public class TupleStreamChunker
        implements BlockIterable<TupleStream>
{
    private final int positionChunkSize;
    private final TupleStream tupleStream;

    public TupleStreamChunker(int positionChunkWidth, TupleStream tupleStream)
    {
        checkArgument(positionChunkWidth > 0, "positionChunkWidth must be positive");
        checkNotNull(tupleStream, "tupleStream is null");

        this.positionChunkSize = positionChunkWidth;
        this.tupleStream = tupleStream;
    }
    
    public static TupleStreamChunker chunk(int positionChunkWidth, TupleStream tupleStream)
    {
        return new TupleStreamChunker(positionChunkWidth, tupleStream);
    }

    @Override
    public BlockIterator<TupleStream> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new ChunkingTupleStreamIterator(positionChunkSize, tupleStream.cursor(session));
    }

    private static class ChunkingTupleStreamIterator
            extends AbstractBlockIterator<TupleStream>
    {
        private final int positionChunkWidth;
        private final Cursor cursor;

        private long nextStartPosition;

        private ChunkingTupleStreamIterator(int positionChunkWidth, Cursor cursor)
        {
            this.positionChunkWidth = positionChunkWidth;
            this.cursor = cursor;
            nextStartPosition = cursor.getRange().getStart();
        }

        @Override
        protected TupleStream computeNext()
        {
            if (nextStartPosition > cursor.getRange().getEnd() || cursor.isFinished()) {
                return endOfData();
            }
            Range rangeChunk = Range.create(nextStartPosition, Math.min(nextStartPosition + positionChunkWidth - 1, cursor.getRange().getEnd()));
            nextStartPosition += positionChunkWidth;
            return new RangeBoundedTupleStream(rangeChunk, cursor);
        }
    }
}
