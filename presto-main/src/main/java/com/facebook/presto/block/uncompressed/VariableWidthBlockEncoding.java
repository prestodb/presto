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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.block.BlockEncodingSerde;
import com.facebook.presto.serde.TypeSerde;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.TypeManager;
import com.facebook.presto.type.VarcharType;
import com.facebook.presto.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<VariableWidthBlockEncoding> FACTORY = new VariableWidthBlockEncodingFactory();
    private static final String NAME = "VARIABLE_WIDTH";

    private final VariableWidthType type;

    public VariableWidthBlockEncoding(Type type)
    {
        this.type = (VarcharType) checkNotNull(type, "type is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        checkArgument(block.getType().equals(type), "Invalid block");

        Slice rawSlice;
        if (block instanceof AbstractVariableWidthRandomAccessBlock) {
            AbstractVariableWidthRandomAccessBlock uncompressedBlock = (AbstractVariableWidthRandomAccessBlock) block;
            rawSlice = uncompressedBlock.getRawSlice();
        }
        else if (block instanceof VariableWidthBlock) {
            VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
            rawSlice = variableWidthBlock.getRawSlice();
        }
        else {
            throw new IllegalArgumentException("Unsupported block type " + block.getClass().getName());
        }

        writeUncompressedBlock(sliceOutput,
                block.getPositionCount(),
                rawSlice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Slice slice = sliceInput.readSlice(blockSize);
        return new VariableWidthBlock(type, positionCount, slice);
    }

    private static void writeUncompressedBlock(SliceOutput destination, int positionCount, Slice slice)
    {
        destination
                .appendInt(slice.length())
                .appendInt(positionCount)
                .writeBytes(slice);
    }

    private static class VariableWidthBlockEncodingFactory
            implements BlockEncodingFactory<VariableWidthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public VariableWidthBlockEncoding readEncoding(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, SliceInput input)
        {
            Type type = TypeSerde.readType(typeManager, input);
            return new VariableWidthBlockEncoding(type);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde blockEncodingSerde, SliceOutput output, VariableWidthBlockEncoding blockEncoding)
        {
            TypeSerde.writeInfo(output, blockEncoding.getType());
        }
    }
}
