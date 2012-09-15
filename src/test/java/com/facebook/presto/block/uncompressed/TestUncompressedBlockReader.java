/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.ingest.ColumnProcessor;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static com.facebook.presto.block.Blocks.assertBlockStreamEquals;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestUncompressedBlockReader
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        UncompressedBlock block = new BlockBuilder(0, TupleInfo.SINGLE_VARBINARY)
                .append("alice".getBytes(UTF_8))
                .append("bob".getBytes(UTF_8))
                .append("charlie".getBytes(UTF_8))
                .append("dave".getBytes(UTF_8))
                .build();

        UncompressedBlockStream blockStream = new UncompressedBlockStream(TupleInfo.SINGLE_VARBINARY, ImmutableList.of(block));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ColumnProcessor processor = new UncompressedColumnWriter(Type.VARIABLE_BINARY, 0, blockStream.cursor(), out);
        processor.processPositions(Integer.MAX_VALUE);
        processor.finish();

        ImmutableList<UncompressedBlock> copiedBlocks = ImmutableList.copyOf(UncompressedSerde.read(Slices.wrappedBuffer(out.toByteArray())));

        // this is only true because the input is small
        assertEquals(copiedBlocks.size(), 1);
        TupleStream copiedBlock = copiedBlocks.get(0);
        assertEquals(copiedBlock.getRange(), block.getRange());

        assertBlockStreamEquals(new UncompressedBlockStream(TupleInfo.SINGLE_VARBINARY, copiedBlocks), blockStream);
    }
}
