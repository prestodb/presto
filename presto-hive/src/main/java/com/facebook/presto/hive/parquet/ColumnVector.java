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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import parquet.column.ColumnDescriptor;
import parquet.column.page.DataPage;
import parquet.column.values.ValuesReader;

public abstract class ColumnVector
{
    protected int numValues;
    protected DataPage[] pages;
    protected ValuesReader[] readers;
    protected Type type;

    public ColumnVector(Type type)
    {
        this.type = type;
    }

    public Type getType()
    {
        return type;
    }

    public abstract Block getBlock();

    public int size()
    {
        return numValues;
    }

    void setNumberOfValues(int numValues)
    {
        this.numValues = numValues;
    }

    void setPages(DataPage[] pages)
    {
        this.pages = pages;
    }

    void setReaders(ValuesReader[] readers)
    {
        this.readers = readers;
    }

    public static final ColumnVector createVector(ColumnDescriptor descriptor, Type type)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return new BooleanColumnVector(type);
            case DOUBLE:
                return new DoubleColumnVector(type);
            case FLOAT:
                return new FloatColumnVector(type);
            case INT32:
                return new IntColumnVector(type);
            case INT64:
                return new LongColumnVector(type);
            case BINARY:
                return new SliceColumnVector(type);
            default:
                throw new IllegalArgumentException("Unhandled column type " + descriptor.getType());
        }
    }
}
