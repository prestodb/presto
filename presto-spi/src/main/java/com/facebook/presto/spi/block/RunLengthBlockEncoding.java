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

import static java.util.Objects.requireNonNull;

public class RunLengthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<RunLengthBlockEncoding> FACTORY = new RunLengthBlockEncodingFactory();
    private static final String NAME = "RLE";

    private final BlockEncoding valueBlockEncoding;

    public RunLengthBlockEncoding(BlockEncoding valueBlockEncoding)
    {
        this.valueBlockEncoding = requireNonNull(valueBlockEncoding, "valueBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
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
        Block value = getValueBlockEncoding().readBlock(sliceInput);

        return new RunLengthEncodedBlock(value, positionCount);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
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
        public RunLengthBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            BlockEncoding valueBlockEncoding = serde.readBlockEncoding(input);
            return new RunLengthBlockEncoding(valueBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, RunLengthBlockEncoding blockEncoding)
        {
            serde.writeBlockEncoding(output, blockEncoding.getValueBlockEncoding());
        }
    }
}
