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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import parquet.column.ColumnDescriptor;
import parquet.column.values.ValuesReader;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public abstract class ParquetBlockBuilder
{
    protected int size;
    protected Type type;
    protected ColumnDescriptor descriptor;
    protected BlockBuilder blockBuilder;

    public ParquetBlockBuilder(int size,
                                Type type,
                                ColumnDescriptor descriptor,
                                BlockBuilder blockBuilder)
    {
        this.size = size;
        this.type = type;
        this.descriptor = descriptor;
        this.blockBuilder = blockBuilder;
    }

    public int getSize()
    {
        return size;
    }

    public Type getType()
    {
        return type;
    }

    public ColumnDescriptor getColumnDescriptor()
    {
        return descriptor;
    }

    public Block buildBlock()
    {
        return blockBuilder.build();
    }

    public void readValues(ValuesReader valuesReader, int valueNumber, ParquetLevelReader definitionReader)
    {
        for (int i = 0; i < valueNumber; i++) {
            if (definitionReader.readLevel() == descriptor.getMaxDefinitionLevel()) {
                readValue(valuesReader);
            }
            else {
                blockBuilder.appendNull();
            }
        }
    }

    public abstract void readValue(ValuesReader valuesReader);

    public static final ParquetBlockBuilder createBlockBuilder(int size, ColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return new ParquetBooleanBuilder(size, descriptor);
            case INT32:
                return new ParquetIntBuilder(size, descriptor);
            case INT64:
                return new ParquetLongBuilder(size, descriptor);
            case FLOAT:
                return new ParquetFloatBuilder(size, descriptor);
            case DOUBLE:
                return new ParquetDoubleBuilder(size, descriptor);
            case BINARY:
                return new ParquetBinaryBuilder(size, descriptor);
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }
}
