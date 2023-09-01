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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.RichColumnDescriptor;

import static java.lang.Float.floatToIntBits;

public class IntColumnReader
        extends AbstractColumnReader
{
    public IntColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (type instanceof BigintType) {
                type.writeLong(blockBuilder, Integer.valueOf(valuesReader.readInteger()).longValue());
                return;
            }
            if (type instanceof RealType) {
                type.writeLong(blockBuilder, floatToIntBits(Integer.valueOf(valuesReader.readInteger()).floatValue()));
                return;
            }
            if (type instanceof DoubleType) {
                type.writeDouble(blockBuilder, Integer.valueOf(valuesReader.readInteger()).doubleValue());
                return;
            }
            type.writeLong(blockBuilder, valuesReader.readInteger());
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readInteger();
        }
    }
}
