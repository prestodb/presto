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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class InterleavedBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<InterleavedBlockEncoding> FACTORY = new InterleavedBlockEncodingFactory();
    private static final String NAME = "INTERLEAVED";

    private final BlockEncoding[] individualBlockEncodings;

    public InterleavedBlockEncoding(BlockEncoding[] individualBlockEncodings)
    {
        this.individualBlockEncodings = individualBlockEncodings;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        AbstractInterleavedBlock interleavedBlock = (AbstractInterleavedBlock) block;

        if (interleavedBlock.getBlockCount() != individualBlockEncodings.length) {
            throw new IllegalArgumentException(
                    "argument block differs in length (" + interleavedBlock.getBlockCount() + ") with this encoding (" + individualBlockEncodings.length + ")");
        }

        Block[] subBlocks = interleavedBlock.computeSerializableSubBlocks();
        for (int i = 0; i < subBlocks.length; i++) {
            individualBlockEncodings[i].writeBlock(sliceOutput, subBlocks[i]);
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block[] individualBlocks = new Block[individualBlockEncodings.length];
        for (int i = 0; i < individualBlockEncodings.length; i++) {
            individualBlocks[i] = individualBlockEncodings[i].readBlock(sliceInput);
        }
        return new InterleavedBlock(individualBlocks);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class InterleavedBlockEncodingFactory
            implements BlockEncodingFactory<InterleavedBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public InterleavedBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            int individualBlockEncodingsCount = input.readInt();
            BlockEncoding[] individualBlockEncodings = new BlockEncoding[individualBlockEncodingsCount];
            for (int i = 0; i < individualBlockEncodingsCount; i++) {
                individualBlockEncodings[i] = serde.readBlockEncoding(input);
            }
            return new InterleavedBlockEncoding(individualBlockEncodings);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, InterleavedBlockEncoding blockEncoding)
        {
            output.appendInt(blockEncoding.individualBlockEncodings.length);
            for (BlockEncoding individualBlockEncoding : blockEncoding.individualBlockEncodings) {
                serde.writeBlockEncoding(output, individualBlockEncoding);
            }
        }
    }
}
