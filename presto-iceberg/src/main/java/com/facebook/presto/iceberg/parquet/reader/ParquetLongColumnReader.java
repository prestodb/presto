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
package com.facebook.presto.iceberg.parquet.reader;

import com.facebook.presto.iceberg.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;

public class ParquetLongColumnReader
        extends ParquetColumnReader
{
    public ParquetLongColumnReader(RichColumnDescriptor descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type type)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (type.equals(TimeType.TIME) || type.equals(TimestampType.TIMESTAMP)) {
                type.writeLong(blockBuilder, valuesReader.readLong() / 1000);
            }
            else if (type.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) || type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
                type.writeLong(blockBuilder, DateTimeEncoding.packDateTimeWithZone(valuesReader.readLong() / 1000, TimeZoneKey.UTC_KEY));
            }
            else {
                type.writeLong(blockBuilder, valuesReader.readLong());
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readLong();
        }
    }
}
