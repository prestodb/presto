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
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import io.airlift.slice.Slices;
import parquet.column.ColumnDescriptor;
import parquet.io.api.Binary;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class ParquetBinaryColumnReader
        extends ParquetColumnReader
{
    public ParquetBinaryColumnReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    public BlockBuilder createBlockBuilder()
    {
        return VARCHAR.createBlockBuilder(new BlockBuilderStatus(), nextBatchSize);
    }

    @Override
    public void readValues(BlockBuilder blockBuilder, int valueNumber)
    {
        for (int i = 0; i < valueNumber; i++) {
            if (definitionReader.readLevel() == columnDescriptor.getMaxDefinitionLevel()) {
                Binary binary = valuesReader.readBytes();
                if (binary.length() == 0) {
                    VARCHAR.writeSlice(blockBuilder, Slices.EMPTY_SLICE);
                }
                else {
                    VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(binary.getBytes()));
                }
            }
            else {
                blockBuilder.appendNull();
            }
        }
    }

    @Override
    public void skipValues(int offsetNumber)
    {
        for (int i = 0; i < offsetNumber; i++) {
            if (definitionReader.readLevel() == columnDescriptor.getMaxDefinitionLevel()) {
                valuesReader.readBytes();
            }
        }
    }
}
