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
package com.facebook.presto.parquet.writer.valuewriter;

import com.facebook.presto.common.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.DateType.DATE;

public class DateValueWriter
        extends PrimitiveValueWriter
{
    public DateValueWriter(Supplier<ValuesWriter> valuesWriterSupplier, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriterSupplier);
    }

    @Override
    public void write(Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                int value = (int) DATE.getLong(block, position);
                getValueWriter().writeInteger(value);
                getStatistics().updateStats(value);
            }
        }
    }
}
