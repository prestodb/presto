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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.serde.TupleInfoSerde;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<VariableWidthBlockEncoding> FACTORY = new VariableWidthBlockEncodingFactory();
    private static final String NAME = "VARIABLE_WIDTH";

    private final TupleInfo tupleInfo;

    public VariableWidthBlockEncoding(TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        this.tupleInfo = tupleInfo;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        checkArgument(block.getTupleInfo().equals(getTupleInfo()), "Invalid tuple info");

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
        return new VariableWidthBlock(new VariableWidthTypeInfo(getTupleInfo().getType()), positionCount, slice);
    }

    private static void writeUncompressedBlock(SliceOutput destination, int tupleCount, Slice slice)
    {
        destination
                .appendInt(slice.length())
                .appendInt(tupleCount)
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
        public VariableWidthBlockEncoding readEncoding(BlockEncodingManager blockEncodingManager, SliceInput input)
        {
            TupleInfo tupleInfo = TupleInfoSerde.readTupleInfo(input);
            return new VariableWidthBlockEncoding(tupleInfo);
        }

        @Override
        public void writeEncoding(BlockEncodingManager blockEncodingManager, SliceOutput output, VariableWidthBlockEncoding blockEncoding)
        {
            TupleInfoSerde.writeTupleInfo(output, blockEncoding.getTupleInfo());
        }
    }
}
