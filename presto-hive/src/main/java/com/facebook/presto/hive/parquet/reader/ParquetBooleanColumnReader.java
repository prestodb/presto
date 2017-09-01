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
import com.facebook.presto.spi.type.Type;
import parquet.column.ColumnDescriptor;

import java.util.Optional;

public class ParquetBooleanColumnReader
        extends ParquetColumnReader
{
    public ParquetBooleanColumnReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, int mapRowNum)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            type.writeBoolean(blockBuilder, valuesReader.readBoolean());
        }
        else {
            handleNull(blockBuilder, isNullAtRowNum, isMapKey, mapRowNum);
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readBoolean();
        }
    }
}
