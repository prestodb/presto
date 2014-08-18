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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRunLengthEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(blockBuilder, "alice");
        Block value = blockBuilder.build();

        RunLengthEncodedBlock expectedBlock = new RunLengthEncodedBlock(value, 11);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        RunLengthBlockEncoding blockEncoding = new RunLengthBlockEncoding(new VariableWidthBlockEncoding());
        blockEncoding.writeBlock(sliceOutput, expectedBlock);
        RunLengthEncodedBlock actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        assertTrue(positionEqualsPosition(VARCHAR, actualBlock, 0, expectedBlock, 0));
        BlockAssertions.assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        Block expectedBlock = expectedBlockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new RunLengthEncoder(sliceOutput, VARCHAR).append(expectedBlock).finish();
        SliceInput sliceInput = sliceOutput.slice().getInput();

        Block block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(positionEqualsPosition(VARCHAR, rleBlock, 0, expectedBlock, 0));
        assertEquals(rleBlock.getPositionCount(), 2);
        assertEquals(VARCHAR.getSlice(rleBlock, 0).toStringUtf8(), "alice");

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(positionEqualsPosition(VARCHAR, rleBlock, 0, expectedBlock, 2));
        assertEquals(rleBlock.getPositionCount(), 4);
        assertEquals(VARCHAR.getSlice(rleBlock, 0).toStringUtf8(), "bob");

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertTrue(positionEqualsPosition(VARCHAR, rleBlock, 0, expectedBlock, 6));
        assertEquals(rleBlock.getPositionCount(), 6);
        assertEquals(VARCHAR.getSlice(rleBlock, 0).toStringUtf8(), "charlie");

        assertFalse(sliceInput.isReadable());
    }
}
