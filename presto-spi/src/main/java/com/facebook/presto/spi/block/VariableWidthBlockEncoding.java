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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    private final VariableWidthType type;

    public VariableWidthBlockEncoding(Type type)
    {
        this.type = (VariableWidthType) requireNonNull(type, "type is null");
    }

    @Override
    public String getName()
    {
        return type.getName();
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        if (!block.getType().equals(type)) {
            throw new IllegalArgumentException("Invalid block");
        }

        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        AbstractVariableWidthRandomAccessBlock uncompressedBlock = (AbstractVariableWidthRandomAccessBlock) block;

        Slice rawSlice = uncompressedBlock.getRawSlice();

        int positionCount = uncompressedBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount; position++) {
            sliceOutput.appendInt(uncompressedBlock.getPositionOffset(position));
        }

        sliceOutput
                .appendInt(rawSlice.length())
                .writeBytes(rawSlice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            offsets[position] = sliceInput.readInt();
        }

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new VariableWidthRandomAccessBlock(type, positionCount, slice, offsets);
    }

    public static class VariableWidthBlockEncodingFactory
            implements BlockEncodingFactory<VariableWidthBlockEncoding>
    {
        private final Type type;

        public VariableWidthBlockEncodingFactory(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public String getName()
        {
            return type.getName();
        }

        @Override
        public VariableWidthBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new VariableWidthBlockEncoding(type);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, VariableWidthBlockEncoding blockEncoding)
        {
        }
    }
}
