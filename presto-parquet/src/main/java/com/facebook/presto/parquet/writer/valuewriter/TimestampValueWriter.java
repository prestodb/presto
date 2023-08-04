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
import com.facebook.presto.common.type.Type;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TimestampValueWriter
        extends PrimitiveValueWriter
{
    private final Type type;
    private final boolean writeMicroseconds;

    public TimestampValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        this.type = requireNonNull(type, "type is null");
        this.writeMicroseconds = parquetType.isPrimitive() && parquetType.getOriginalType() == OriginalType.TIMESTAMP_MICROS;
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                long value = type.getLong(block, i);
                long scaledValue = writeMicroseconds ? MILLISECONDS.toMicros(value) : value;
                getValueWriter().writeLong(scaledValue);
                getStatistics().updateStats(scaledValue);
            }
        }
    }
}
