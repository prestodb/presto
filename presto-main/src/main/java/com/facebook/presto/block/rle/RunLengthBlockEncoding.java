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
package com.facebook.presto.block.rle;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.type.Type;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public class RunLengthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<RunLengthBlockEncoding> FACTORY = new RunLengthBlockEncodingFactory();
    private static final String NAME = "RLE";

    private final BlockEncoding valueBlockEncoding;

    public RunLengthBlockEncoding(BlockEncoding valueBlockEncoding)
    {
        this.valueBlockEncoding = checkNotNull(valueBlockEncoding, "valueBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type getType()
    {
        return valueBlockEncoding.getType();
    }

    public BlockEncoding getValueBlockEncoding()
    {
        return valueBlockEncoding;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;

        // write the run length
        sliceOutput.writeInt(rleBlock.getPositionCount());

        // write the value
        getValueBlockEncoding().writeBlock(sliceOutput, rleBlock.getValue());
    }

    @Override
    public RunLengthEncodedBlock readBlock(SliceInput sliceInput)
    {
        // read the run length
        int positionCount = sliceInput.readInt();

        // read the value
        RandomAccessBlock value = getValueBlockEncoding().readBlock(sliceInput).toRandomAccessBlock();

        return new RunLengthEncodedBlock(value, positionCount);
    }

    private static class RunLengthBlockEncodingFactory
            implements BlockEncodingFactory<RunLengthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public RunLengthBlockEncoding readEncoding(BlockEncodingManager blockEncodingManager, SliceInput input)
        {
            BlockEncoding valueBlockEncoding = blockEncodingManager.readBlockEncoding(input);
            return new RunLengthBlockEncoding(valueBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingManager blockEncodingManager, SliceOutput output, RunLengthBlockEncoding blockEncoding)
        {
            blockEncodingManager.writeBlockEncoding(output, blockEncoding.getValueBlockEncoding());
        }
    }
}
