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

import static java.util.Objects.requireNonNull;

public class MappedBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<MappedBlockEncoding> FACTORY = new MappedBlockEncodingFactory();
    private static final String NAME = "MASKED_BLOCK";

    private final BlockEncoding underlyingEncoding;

    public MappedBlockEncoding(BlockEncoding underlyingEncoding)
    {
        this.underlyingEncoding = requireNonNull(underlyingEncoding, "underlyingEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        //TODO: how to directly write MaskedBlock to sliceOutput without copying it's content?
        underlyingEncoding.writeBlock(sliceOutput, block.copyRegion(0, block.getPositionCount()));
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        return underlyingEncoding.readBlock(sliceInput);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public BlockEncoding getUnderlyingEncoding()
    {
        return underlyingEncoding;
    }

    public static class MappedBlockEncodingFactory
            implements BlockEncodingFactory<MappedBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public MappedBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            BlockEncoding underlyingEncoding = serde.readBlockEncoding(input);
            return new MappedBlockEncoding(underlyingEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, MappedBlockEncoding blockEncoding)
        {
            serde.writeBlockEncoding(output, blockEncoding.getUnderlyingEncoding());
        }
    }
}
