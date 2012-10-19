package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.GenericCursor;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;

import static com.facebook.presto.block.Cursor.AdvanceResult;

public class MaterializingTupleStream
    implements TupleStream, YieldingIterable<TupleStream>
{
    private final TupleStream delegate;
    private final DataSize blockSize;
    private final double storageMultiplier;

    public MaterializingTupleStream(TupleStream delegate, DataSize blockSize, double storageMultiplier)
    {
        Preconditions.checkNotNull(delegate, "delegate is null");
        Preconditions.checkArgument(storageMultiplier > 0, "storageMultiplier must be greater than zero");

        this.delegate = delegate;
        this.blockSize = blockSize;
        this.storageMultiplier = storageMultiplier;
    }

    public MaterializingTupleStream(TupleStream delegate)
    {
        this(delegate, BlockBuilder.DEFAULT_MAX_BLOCK_SIZE, BlockBuilder.DEFAULT_STORAGE_MULTIPLIER);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return delegate.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return delegate.getRange();
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        return new GenericCursor(session, delegate.getTupleInfo(), iterator(session));
    }

    @Override
    public YieldingIterator<TupleStream> iterator(QuerySession session)
    {
        final Cursor cursor = delegate.cursor(session);
        return new AbstractYieldingIterator<TupleStream>() {
            private long position;

            @Override
            protected TupleStream computeNext()
            {
                // TODO: should we respect the underlying positions? right now, it will reindex everything packed from position zero
                BlockBuilder blockBuilder = new BlockBuilder(position, delegate.getTupleInfo(), blockSize, storageMultiplier);
                do {
                    AdvanceResult result = cursor.advanceNextPosition();
                    switch (result) {
                        case SUCCESS:
                            Cursors.appendCurrentTupleToBlockBuilder(cursor, blockBuilder);
                            position++;
                            break;
                        case MUST_YIELD:
                            if (!blockBuilder.isEmpty()) {
                                return blockBuilder.build();
                            }
                            return setMustYield();
                        case FINISHED:
                            if (!blockBuilder.isEmpty()) {
                                return blockBuilder.build();
                            }
                            return endOfData();
                        default:
                            throw new AssertionError("unknown result: " + result);
                    }
                } while (!blockBuilder.isFull());

                return blockBuilder.build();
            }
        };
    }
}
