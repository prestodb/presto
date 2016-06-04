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
import com.facebook.presto.spi.type.Type;
import parquet.column.ColumnDescriptor;

public class ParquetLongColumnReader
        extends ParquetColumnReader
{
    public ParquetLongColumnReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    public BlockBuilder createBlockBuilder(Type type)
    {
        return type.createBlockBuilder(new BlockBuilderStatus(), nextBatchSize);
    }

    @Override
    public void readValues(BlockBuilder blockBuilder, int valueNumber, Type type)
    {
        for (int i = 0; i < valueNumber; i++) {
            if (definitionReader.readLevel() == columnDescriptor.getMaxDefinitionLevel()) {
                type.writeLong(blockBuilder, valuesReader.readLong());
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
                valuesReader.readLong();
            }
        }
    }
}
