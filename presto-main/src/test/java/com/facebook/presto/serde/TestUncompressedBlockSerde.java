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
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class TestUncompressedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        UncompressedBlock expectedBlock = new BlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new UncompressedBlockEncoding(SINGLE_VARBINARY);
        blockEncoding.writeBlock(sliceOutput, expectedBlock);
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        BlockAssertions.assertBlockEquals(actualBlock, expectedBlock);
    }

    @Test
    public void testCreateBlockWriter()
    {
        ImmutableList<Tuple> tuples = ImmutableList.of(createTuple("alice"),
                createTuple("bob"),
                createTuple("charlie"),
                createTuple("dave"));

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new UncompressedEncoder(sliceOutput).append(tuples).append(tuples).finish();
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());
        BlockAssertions.assertBlockEquals(actualBlock, new BlockBuilder(SINGLE_VARBINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build());
    }
}
