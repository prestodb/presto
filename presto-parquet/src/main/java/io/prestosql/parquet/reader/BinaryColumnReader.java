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
package io.prestosql.parquet.reader;

import io.airlift.slice.Slice;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import parquet.io.api.Binary;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.spi.type.Varchars.truncateToLength;

public class BinaryColumnReader
        extends PrimitiveColumnReader
{
    public BinaryColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
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
        else if (isValueNull()) {
            blockBuilder.appendNull();
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
