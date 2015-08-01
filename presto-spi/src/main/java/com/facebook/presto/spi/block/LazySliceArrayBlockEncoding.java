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

public class LazySliceArrayBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<LazySliceArrayBlockEncoding> FACTORY = new LazySliceArrayBlockEncodingFactory();
    private static final String NAME = "LAZY_SLICE_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        boolean isDictionary = sliceInput.readBoolean();
        if (isDictionary) {
            return new DictionaryBlockEncoding(new SliceArrayBlockEncoding()).readBlock(sliceInput);
        }
        return new SliceArrayBlockEncoding().readBlock(sliceInput);
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        LazySliceArrayBlock sliceArrayBlock = (LazySliceArrayBlock) block;

        // is dictionary block
        sliceOutput.writeBoolean(sliceArrayBlock.isDictionary());
        Block nonLazyBlock = sliceArrayBlock.createNonLazyBlock();
        nonLazyBlock.getEncoding().writeBlock(sliceOutput, nonLazyBlock);
    }

    @Override
    public int getEstimatedSize(Block block)
    {
        int positionCount = block.getPositionCount();

        int size = 4; // positionCount integer bytes
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                totalLength += block.getLength(position);
            }
            size += 4; // length integer bytes
        }

        size += positionCount / 8 + 1; // one byte null bits per eight elements and possibly last null bits
        size += totalLength;

        return size;
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class LazySliceArrayBlockEncodingFactory
            implements BlockEncodingFactory<LazySliceArrayBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public LazySliceArrayBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new LazySliceArrayBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, LazySliceArrayBlockEncoding blockEncoding)
        {
        }
    }
}
