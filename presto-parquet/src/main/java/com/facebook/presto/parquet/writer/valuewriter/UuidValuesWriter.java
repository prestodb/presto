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

import com.facebook.airlift.concurrent.NotThreadSafe;
import com.facebook.presto.common.block.Block;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

@NotThreadSafe
public class UuidValuesWriter
        extends PrimitiveValueWriter
{
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(2 * SIZE_OF_LONG)
            .order(ByteOrder.BIG_ENDIAN);

    public UuidValuesWriter(Supplier<ValuesWriter> valuesWriterSupplier, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriterSupplier);
    }

    @Override
    public void write(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                writeBuffer.clear();
                writeBuffer.putLong(block.getLong(i, 0));
                writeBuffer.putLong(block.getLong(i, SIZE_OF_LONG));
                writeBuffer.flip();
                Binary data = Binary.fromReusedByteBuffer(writeBuffer);
                getValueWriter().writeBytes(data);
                getStatistics().updateStats(data);
            }
        }
    }
}
