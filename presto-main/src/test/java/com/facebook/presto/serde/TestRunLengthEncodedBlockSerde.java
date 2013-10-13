/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRunLengthEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        RandomAccessBlock value = createBlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .build()
                .toRandomAccessBlock();

        RunLengthEncodedBlock expectedBlock = new RunLengthEncodedBlock(value, 11);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        RunLengthBlockEncoding blockEncoding = new RunLengthBlockEncoding(new UncompressedBlockEncoding(SINGLE_VARBINARY));
        blockEncoding.writeBlock(sliceOutput, expectedBlock);
        RunLengthEncodedBlock actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        assertTrue(actualBlock.equals(0, expectedBlock, 0));
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        RandomAccessBlock expectedBlock = createBlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("alice")
                .append("bob")
                .append("bob")
                .append("bob")
                .append("bob")
                .append("charlie")
                .append("charlie")
                .append("charlie")
                .append("charlie")
                .append("charlie")
                .append("charlie")
                .build()
                .toRandomAccessBlock();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new RunLengthEncoder(sliceOutput).append(expectedBlock).finish();
        SliceInput sliceInput = sliceOutput.slice().getInput();

        Block block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(rleBlock.equals(0, expectedBlock, 0));
        assertEquals(rleBlock.getPositionCount(), 2);

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(rleBlock.equals(0, expectedBlock, 2));
        assertEquals(rleBlock.getPositionCount(), 4);

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(rleBlock.equals(0, expectedBlock, 6));
        assertEquals(rleBlock.getPositionCount(), 6);

        assertFalse(sliceInput.isReadable());
    }
}
