/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestUncompressedBlockSerde
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        BlockBuilder builder = new BlockBuilder(0, new TupleInfo(Type.VARIABLE_BINARY));
        ValueBlock block = builder.append("alice".getBytes(UTF_8))
                .append("bob".getBytes(UTF_8))
                .append("charlie".getBytes(UTF_8))
                .append("dave".getBytes(UTF_8))
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        UncompressedBlockSerde.write(ImmutableList.of(block).iterator(), out);

        ImmutableList<ValueBlock> copiedBlocks = ImmutableList.copyOf(UncompressedBlockSerde.read(Slices.wrappedBuffer(out.toByteArray())));

        // this is only true because the input is small
        assertEquals(copiedBlocks.size(), 1);
        ValueBlock copiedBlock = copiedBlocks.get(0);
        assertEquals(copiedBlock.getRange(), block.getRange());
        assertTrue(Iterators.elementsEqual(copiedBlock.iterator(), block.iterator()));
    }
}
