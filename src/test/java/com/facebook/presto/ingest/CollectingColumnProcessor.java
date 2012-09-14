package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlockStream;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

public class CollectingColumnProcessor
        extends AbstractColumnProcessor
{
    private final BlockBuilder builder;
    private BlockStream blockStream;

    CollectingColumnProcessor(Type type, int index, Cursor cursor)
    {
        super(type, index, cursor);
        this.builder = new BlockBuilder(0, new TupleInfo(type));
    }

    public BlockStream getBlockStream()
    {
        checkState(blockStream != null, "close not called");
        return blockStream;
    }

    @Override
    public boolean processPositions(long end)
            throws IOException
    {
        while (cursor.advanceNextPosition()) {
            switch (type) {
                case FIXED_INT_64:
                    builder.append(cursor.getLong(index));
                    break;
                case DOUBLE:
                    builder.append(cursor.getDouble(index));
                    break;
                case VARIABLE_BINARY:
                    builder.append(cursor.getSlice(index));
                    break;
                default:
                    throw new AssertionError("unhandled type: " + type);
            }
            if (cursor.getPosition() >= end) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void finished()
            throws IOException
    {
        blockStream = new UncompressedBlockStream(new TupleInfo(type), ImmutableList.of(builder.build()));
    }
}
