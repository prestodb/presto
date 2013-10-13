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
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class UncompressedBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<UncompressedBlockEncoding> FACTORY = new UncompressedBlockEncodingFactory();
    private static final String NAME = "RAW";

    private final TupleInfo tupleInfo;

    public UncompressedBlockEncoding(TupleInfo tupleInfo)
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
        Preconditions.checkArgument(block.getTupleInfo().equals(tupleInfo), "Invalid tuple info");
        writeUncompressedBlock(sliceOutput,
                block.getPositionCount(),
                block.getRawSlice());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Slice slice = sliceInput.readSlice(blockSize);
        Type type = tupleInfo.getType();
        if (type.isFixedSize()) {
            return new FixedWidthBlock(new FixedWidthTypeInfo(type), positionCount, slice);
        }
        else {
            return new VariableWidthBlock(new VariableWidthTypeInfo(type), positionCount, slice);
        }
    }

    private static void writeUncompressedBlock(SliceOutput destination, int tupleCount, Slice slice)
    {
        destination
                .appendInt(slice.length())
                .appendInt(tupleCount)
                .writeBytes(slice);
    }

    private static class UncompressedBlockEncodingFactory
            implements BlockEncodingFactory<UncompressedBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public UncompressedBlockEncoding readEncoding(BlockEncodingManager blockEncodingManager, SliceInput input)
        {
            TupleInfo tupleInfo = TupleInfoSerde.readTupleInfo(input);
            return new UncompressedBlockEncoding(tupleInfo);
        }

        @Override
        public void writeEncoding(BlockEncodingManager blockEncodingManager, SliceOutput output, UncompressedBlockEncoding blockEncoding)
        {
            TupleInfoSerde.writeTupleInfo(output, blockEncoding.tupleInfo);
        }
    }
}
