/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.serde.BlockSerdeSerde.readBlockSerde;
import static com.facebook.presto.serde.BlockSerdeSerde.writeBlockSerde;
import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;
import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestBlockSerdeSerde {
    @Test
    public void testRoundTrip()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writeBlockSerde(sliceOutput, UNCOMPRESSED_BLOCK_SERDE);
        assertInstanceOf(readBlockSerde(sliceOutput.slice().getInput()), UncompressedBlockSerde.class);
    }
}
