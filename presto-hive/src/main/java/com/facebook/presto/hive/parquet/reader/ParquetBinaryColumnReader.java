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
import io.airlift.slice.Slice;
import parquet.column.ColumnDescriptor;
import parquet.io.api.Binary;

import java.util.Optional;

import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;

public class ParquetBinaryColumnReader
        extends ParquetColumnReader
{
    public ParquetBinaryColumnReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type, Optional<boolean[]> isNullAtRowNum, boolean isMapKey, int mapRowNum)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            Binary binary = valuesReader.readBytes();
            Slice value;
            if (binary.length() == 0) {
                value = EMPTY_SLICE;
            }
            else {
                value = wrappedBuffer(binary.getBytes());
            }
            if (isVarcharType(type)) {
                value = truncateToLength(value, type);
            }
            if (isCharType(type)) {
                value = truncateToLengthAndTrimSpaces(value, type);
            }
            type.writeSlice(blockBuilder, value);
        }
        else {
            handleNull(blockBuilder, isNullAtRowNum, isMapKey, mapRowNum);
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readBytes();
        }
    }
}
