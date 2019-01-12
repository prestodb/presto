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

package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class SingleRowBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ROW_ELEMENT";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        SingleRowBlock singleRowBlock = (SingleRowBlock) block;
        int numFields = singleRowBlock.getNumFields();
        int rowIndex = singleRowBlock.getRowIndex();
        sliceOutput.appendInt(numFields);
        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(sliceOutput, singleRowBlock.getRawFieldBlock(i).getRegion(rowIndex, 1));
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < fieldBlocks.length; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }
        return new SingleRowBlock(0, fieldBlocks);
    }
}
