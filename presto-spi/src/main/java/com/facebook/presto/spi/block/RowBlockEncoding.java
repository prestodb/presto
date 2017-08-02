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

import static io.airlift.slice.Slices.wrappedIntArray;
import static java.util.Objects.requireNonNull;

public class RowBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<RowBlockEncoding> FACTORY = new RowBlockEncodingFactory();
    private static final String NAME = "ROW";

    private final BlockEncoding[] fieldBlockEncodings;

    public RowBlockEncoding(BlockEncoding[] fieldBlockEncodings)
    {
        this.fieldBlockEncodings = requireNonNull(fieldBlockEncodings, "fieldBlockEncodings is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        AbstractRowBlock rowBlock = (AbstractRowBlock) block;

        if (rowBlock.numFields != fieldBlockEncodings.length) {
            throw new IllegalArgumentException(
                    "argument block differs in length (" + rowBlock.numFields + ") with this encoding (" + fieldBlockEncodings.length + ")");
        }

        int positionCount = rowBlock.getPositionCount();

        int offsetBase = rowBlock.getOffsetBase();
        int[] fieldBlockOffsets = rowBlock.getFieldBlockOffsets();

        int startFieldBlockOffset = fieldBlockOffsets[offsetBase];
        int endFieldBlockOffset = fieldBlockOffsets[offsetBase + positionCount];
        for (int i = 0; i < fieldBlockEncodings.length; i++) {
            fieldBlockEncodings[i].writeBlock(sliceOutput, rowBlock.getFieldBlocks()[i].getRegion(startFieldBlockOffset, endFieldBlockOffset - startFieldBlockOffset));
        }

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(fieldBlockOffsets[offsetBase + position] - startFieldBlockOffset);
        }
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block[] fieldBlocks = new Block[fieldBlockEncodings.length];
        for (int i = 0; i < fieldBlockEncodings.length; i++) {
            fieldBlocks[i] = fieldBlockEncodings[i].readBlock(sliceInput);
        }

        int positionCount = sliceInput.readInt();
        int[] fieldBlockOffsets = new int[positionCount + 1];
        sliceInput.readBytes(wrappedIntArray(fieldBlockOffsets));
        boolean[] rowIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        return new RowBlock(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class RowBlockEncodingFactory
            implements BlockEncodingFactory<RowBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public RowBlockEncoding readEncoding(TypeManager typeManager, BlockEncodingSerde serde, SliceInput input)
        {
            int numFields = input.readInt();
            BlockEncoding[] fieldBlockEncodings = new BlockEncoding[numFields];
            for (int i = 0; i < numFields; i++) {
                fieldBlockEncodings[i] = serde.readBlockEncoding(input);
            }
            return new RowBlockEncoding(fieldBlockEncodings);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, RowBlockEncoding blockEncoding)
        {
            output.appendInt(blockEncoding.fieldBlockEncodings.length);
            for (BlockEncoding fieldBlockEncoding : blockEncoding.fieldBlockEncodings) {
                serde.writeBlockEncoding(output, fieldBlockEncoding);
            }
        }
    }
}
