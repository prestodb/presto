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
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.FixedWidthBlock;
import com.facebook.presto.block.uncompressed.VariableWidthRandomAccessBlock;
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TypeInfo;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class RunLengthBlockEncoding
        implements BlockEncoding
{
    private final TupleInfo tupleInfo;

    public RunLengthBlockEncoding(TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        this.tupleInfo = tupleInfo;
    }

    public RunLengthBlockEncoding(SliceInput input)
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
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        sliceOutput.appendInt(rleBlock.getRawSlice().length())
                .appendInt(rleBlock.getPositionCount())
                .writeBytes(rleBlock.getRawSlice());
    }

    @Override
    public RunLengthEncodedBlock readBlock(SliceInput sliceInput)
    {
        int tupleLength = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();

        Slice slice = sliceInput.readSlice(tupleLength);

        RandomAccessBlock value;
        TypeInfo typeInfo = tupleInfo.getTypeInfo();
        if (typeInfo instanceof FixedWidthTypeInfo) {
            value = new FixedWidthBlock((FixedWidthTypeInfo) typeInfo, 1, slice);
        }
        else {
            value = new VariableWidthRandomAccessBlock((VariableWidthTypeInfo) typeInfo, 1, slice);
        }

        return new RunLengthEncodedBlock(value, tupleCount);
    }

    public static void serialize(SliceOutput output, RunLengthBlockEncoding encoding)
    {
        TupleInfoSerde.writeTupleInfo(output, encoding.tupleInfo);
    }
}
