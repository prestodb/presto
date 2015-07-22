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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static java.lang.Integer.max;

public class BlackHoleRecordCursor
        implements RecordCursor
{
    // it might not be the best idea to return here array of zero bytes for varchars
    // so instead we are using some other random number that is valid ASCII character
    private static final byte[] CONSTANT_BYTES = new byte[] {42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42};
    private static final Slice CONSTANT_SLICE = Slices.wrappedBuffer(CONSTANT_BYTES);
    private final List<Type> types;
    private final int rows;
    private final long rowSize;

    // incremented to zero before first write
    private int currentRow = -1;

    public BlackHoleRecordCursor(List<Type> types, int rows)
    {
        this.types = types;
        this.rows = rows;
        this.rowSize = sizeOf(types);
    }

    @Override
    public long getTotalBytes()
    {
        return rowSize * rows;
    }

    @Override
    public long getCompletedBytes()
    {
        return rowSize * max(0, currentRow);
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return types.get(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentRow++;
        return currentRow < rows;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        return CONSTANT_SLICE;
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
    }

    private static long sizeOf(List<Type> types)
    {
        long size = 0;
        for (Type type : types) {
            if (type.getJavaType() == boolean.class) {
                size++;
            }
            else if (type.getJavaType() == long.class) {
                size += 8;
            }
            else if (type.getJavaType() == double.class) {
                size += 8;
            }
            else if (type.getJavaType() == Slice.class) {
                size += CONSTANT_SLICE.length();
            }
            else if (type.getJavaType() == byte[].class) {
                size += CONSTANT_BYTES.length;
            }
            else {
                throw new UnsupportedOperationException("Unknown type");
            }
        }
        return size;
    }
}
