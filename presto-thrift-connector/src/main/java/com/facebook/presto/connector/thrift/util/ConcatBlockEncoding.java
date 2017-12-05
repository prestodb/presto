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
package com.facebook.presto.connector.thrift.util;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.DictionaryBlockEncoding;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public class ConcatBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<DictionaryBlockEncoding> FACTORY = new DictionaryBlockEncoding.DictionaryBlockEncodingFactory();
    private static final String NAME = "DICTIONARY";
    private final BlockEncoding concatBlockEncoding;

    public ConcatBlockEncoding(BlockEncoding concatBlockEncoding)
    {
        this.concatBlockEncoding = requireNonNull(concatBlockEncoding, "dictionaryEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        ConcatBlock concatBlock = (ConcatBlock) block;

        // blockCount
        sliceOutput.appendInt(concatBlock.getBlocks().size());

        // underly block
        for (Block internalBlock : concatBlock.getBlocks()) {
            concatBlockEncoding.writeBlock(sliceOutput, internalBlock);
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        // positionCount
        int blockCount = sliceInput.readInt();
        Block[] blocks = new Block[blockCount];
        for (int i = 0; i < blockCount; i++) {
            Block block = concatBlockEncoding.readBlock(sliceInput);
            blocks[i] = block;
        }

        return new ConcatBlock(blocks);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public BlockEncoding getConcatBlockEncoding()
    {
        return concatBlockEncoding;
    }

    public static class ConcatBlockEncodingFactory
            implements BlockEncodingFactory<ConcatBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public ConcatBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            BlockEncoding blockEncoding = serde.readBlockEncoding(input);
            return new ConcatBlockEncoding(blockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, ConcatBlockEncoding blockEncoding)
        {
            serde.writeBlockEncoding(output, blockEncoding.getConcatBlockEncoding());
        }
    }
}
