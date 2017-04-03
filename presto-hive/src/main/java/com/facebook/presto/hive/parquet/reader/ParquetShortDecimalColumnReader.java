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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import parquet.column.ColumnDescriptor;

import static com.facebook.presto.hive.util.DecimalUtils.getShortDecimalValue;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ParquetShortDecimalColumnReader
        extends ParquetColumnReader
{
    ParquetShortDecimalColumnReader(ColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long decimalValue;
            if (columnDescriptor.getType().equals(INT32)) {
                HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(HiveDecimal.create(valuesReader.readInteger()));
                decimalValue = getShortDecimalValue(hiveDecimalWritable, ((DecimalType) type).getScale());
            }
            else if (columnDescriptor.getType().equals(INT64)) {
                HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(HiveDecimal.create(valuesReader.readLong()));
                decimalValue = getShortDecimalValue(hiveDecimalWritable, ((DecimalType) type).getScale());
            }
            else {
                decimalValue = getShortDecimalValue(valuesReader.readBytes().getBytes());
            }
            type.writeLong(blockBuilder, decimalValue);
        }
        else {
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
