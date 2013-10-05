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
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.block.uncompressed.VariableWidthBlock;
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
    private final TupleInfo tupleInfo;

    public UncompressedBlockEncoding(TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        this.tupleInfo = tupleInfo;
    }

    public UncompressedBlockEncoding(SliceInput input)
    {
        Preconditions.checkNotNull(input, "input is null");
        tupleInfo = TupleInfoSerde.readTupleInfo(input);
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

    public static void serialize(SliceOutput output, UncompressedBlockEncoding encoding)
    {
        TupleInfoSerde.writeTupleInfo(output, encoding.tupleInfo);
    }
}
