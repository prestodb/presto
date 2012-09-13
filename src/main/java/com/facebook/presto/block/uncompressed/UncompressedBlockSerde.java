/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.BlockStreamSerde;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Block;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.OutputStreamSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.facebook.presto.SizeOf.SIZE_OF_INT;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
        implements BlockStreamSerde<UncompressedValueBlock>
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();
    private static final UncompressedBlockSerde INSTANCE = new UncompressedBlockSerde();

    @Override
    public void serialize(BlockStream<? extends Block> blockStream, SliceOutput sliceOutput)
    {
        write(blockStream.cursor(), sliceOutput);
    }

    @Override
    public BlockStream<UncompressedValueBlock> deserialize(Slice slice)
    {
        return readAsStream(slice);
    }

    public static void write(Cursor cursor, SliceOutput out)
    {
        // todo We should be able to take advantage of the fact that the cursor might already be over uncompressed blocks and just write them down as they come.
        UncompressedTupleInfoSerde.serialize(cursor.getTupleInfo(), new OutputStreamSliceOutput(out));
        DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);

        int tupleCount = 0;
        while (cursor.advanceNextPosition()) {
            cursor.getTuple().writeTo(buffer);
            tupleCount++;

            if (buffer.size() > MAX_BLOCK_SIZE) {
                write(out, tupleCount, buffer.slice());
                tupleCount = 0;
                buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
            }
        }
        if (buffer.size() > 0) {
            write(out, tupleCount, buffer.slice());
        }
    }

    private static void write(SliceOutput destination, int tupleCount, Slice slice)
    {
        ByteArraySlice header = Slices.allocate(SIZE_OF_INT + SIZE_OF_INT);
        header.output()
                .appendInt(slice.length())
                .appendInt(tupleCount);
        destination.writeBytes(header);
        destination.writeBytes(slice);
    }

    public static BlockStream<UncompressedValueBlock> read(File file)
            throws IOException
    {
        Slice mappedSlice = Slices.mapFileReadOnly(file);
        return INSTANCE.deserialize(mappedSlice);
    }

    public static Iterator<UncompressedValueBlock> read(Slice slice)
    {
        return new UncompressedReader(slice);
    }

    public static BlockStream<UncompressedValueBlock> readAsStream(final Slice slice)
    {
        UncompressedReader reader = new UncompressedReader(slice);

        return new UncompressedBlockStream(reader.tupleInfo, new Iterable<UncompressedValueBlock>()
        {
            @Override
            public Iterator<UncompressedValueBlock> iterator()
            {
                return new UncompressedReader(slice);
            }
        });
    }

    private static class UncompressedReader extends AbstractIterator<UncompressedValueBlock>
    {
        private final TupleInfo tupleInfo;
        private final SliceInput sliceInput;
        private int position;

        private UncompressedReader(Slice slice)
        {
            sliceInput = slice.input();
            this.tupleInfo = UncompressedTupleInfoSerde.deserialize(sliceInput);
        }

        protected UncompressedValueBlock computeNext()
        {
            if (!sliceInput.isReadable()) {
                endOfData();
                return null;
            }

            int blockSize = sliceInput.readInt();
            int tupleCount = sliceInput.readInt();

            Range range = Range.create(position, position + tupleCount - 1);
            position += tupleCount;

            Slice block = sliceInput.readSlice(blockSize);
            return new UncompressedValueBlock(range, tupleInfo, block);
        }
    }
}
