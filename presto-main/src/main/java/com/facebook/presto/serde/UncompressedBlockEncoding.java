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
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
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
        UncompressedBlock uncompressedBlock = (UncompressedBlock) block;
        Preconditions.checkArgument(block.getTupleInfo().equals(tupleInfo), "Invalid tuple info");
        writeUncompressedBlock(sliceOutput,
                uncompressedBlock.getPositionCount(),
                uncompressedBlock.getSlice());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();

        Slice block = sliceInput.readSlice(blockSize);
        return new UncompressedBlock(tupleCount, tupleInfo, block);
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
