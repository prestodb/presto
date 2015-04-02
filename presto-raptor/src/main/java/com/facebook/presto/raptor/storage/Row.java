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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class Row
{
    private final List<Object> columns;
    private final int sizeInBytes;

    public Row(List<Object> columns, int sizeInBytes)
    {
        this.columns = requireNonNull(columns, "columns is null");
        checkArgument(sizeInBytes >= 0, "sizeInBytes must be >= 0");
        this.sizeInBytes = sizeInBytes;
    }

    public List<Object> getColumns()
    {
        return columns;
    }

    public int getSizeInBytes()
    {
        return sizeInBytes;
    }

    public static Row extractRow(Page page, int position, List<Type> types)
    {
        checkArgument(page.getChannelCount() == types.size(), "channelCount does not match");
        checkArgument(position < page.getPositionCount(), "Requested position %s from a page with positionCount %s ", position, page.getPositionCount());

        RowBuilder rowBuilder = new RowBuilder();
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            Type type = types.get(channel);
            if (block.isNull(position)) {
                rowBuilder.add(null, SIZE_OF_BYTE);
            }
            else if (type.getJavaType() == boolean.class) {
                rowBuilder.add(type.getBoolean(block, position), SIZE_OF_BYTE);
            }
            else if (type.getJavaType() == long.class) {
                rowBuilder.add(type.getLong(block, position), SIZE_OF_LONG);
            }
            else if (type.getJavaType() == double.class) {
                rowBuilder.add(type.getDouble(block, position), SIZE_OF_DOUBLE);
            }
            else if (type.getJavaType() == Slice.class) {
                byte[] bytes = type.getSlice(block, position).getBytes();
                Object value = type.equals(VarcharType.VARCHAR) ? new String(bytes) : bytes;
                rowBuilder.add(value, bytes.length);
            }
            else {
                throw new AssertionError("unimplemented type: " + type);
            }
        }
        Row row = rowBuilder.build();
        verify(row.getColumns().size() == types.size(), "Column count in row: %s Expected column count: %s", row.getColumns().size(), types.size());
        return row;
    }

    private static class RowBuilder
    {
        private int rowSize;
        private final List<Object> columns;

        public RowBuilder()
        {
            this.columns = new ArrayList<>();
        }

        public void add(Object value, int size)
        {
            columns.add(value);
            rowSize += size;
        }

        public Row build()
        {
            return new Row(columns, rowSize);
        }
    }
}
