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
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.Tuples;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestRunLengthEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        RunLengthEncodedBlock expectedBlock = new RunLengthEncodedBlock(Tuples.createTuple("alice"), 11);

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        RunLengthBlockEncoding blockEncoding = new RunLengthBlockEncoding(SINGLE_VARBINARY);
        blockEncoding.writeBlock(sliceOutput, expectedBlock);
        RunLengthEncodedBlock actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        assertEquals(actualBlock.getSingleValue(), expectedBlock.getSingleValue());
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        ImmutableList<Tuple> tuples = ImmutableList.of(
                createTuple("alice"),
                createTuple("alice"),
                createTuple("bob"),
                createTuple("bob"),
                createTuple("bob"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("charlie"),
                createTuple("charlie"),
                createTuple("charlie"),
                createTuple("charlie"),
                createTuple("charlie"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new RunLengthEncoder(sliceOutput).append(tuples).finish();
        SliceInput sliceInput = sliceOutput.slice().getInput();

        Block block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("alice"));
        assertEquals(rleBlock.getPositionCount(), 2);

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("bob"));
        assertEquals(rleBlock.getPositionCount(), 4);

        block = blockEncoding.readBlock(sliceInput);
        assertInstanceOf(block, RunLengthEncodedBlock.class);
        rleBlock = (RunLengthEncodedBlock) block;
        assertEquals(rleBlock.getSingleValue(), createTuple("charlie"));
        assertEquals(rleBlock.getPositionCount(), 6);

        assertFalse(sliceInput.isReadable());
    }
}
