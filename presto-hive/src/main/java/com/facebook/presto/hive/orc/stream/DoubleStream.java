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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.orc.Vector;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.readFully;
import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.skipFully;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_DOUBLE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;

public class DoubleStream
{
    private final InputStream input;
    private final byte[] buffer = new byte[Vector.MAX_VECTOR_LENGTH * SIZE_OF_DOUBLE];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public DoubleStream(InputStream input)
            throws IOException
    {
        this.input = input;
    }

    public void skip(long items)
            throws IOException
    {
        long length = items * SIZE_OF_DOUBLE;
        skipFully(input, length);
    }

    public double next()
            throws IOException
    {
        // buffer that umber of values
        readFully(input, buffer, 0, SIZE_OF_DOUBLE);

        return slice.getDouble(0);
    }

    public void nextVector(int items, double[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);
        checkPositionIndex(items, Vector.MAX_VECTOR_LENGTH);

        // buffer that umber of values
        readFully(input, buffer, 0, items * SIZE_OF_DOUBLE);

        // copy values directly into vector
        unsafe.copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, vector, ARRAY_DOUBLE_BASE_OFFSET, items * SIZE_OF_DOUBLE);
    }

    public void nextVector(long items, double[] vector, boolean[] isNull)
            throws IOException
    {
        // count the number of non nulls
        int notNullCount = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                notNullCount++;
            }
        }

        // buffer that umber of values
        readFully(input, buffer, 0, notNullCount * SIZE_OF_DOUBLE);

        // load them into the buffer
        int elementIndex = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = slice.getDouble(elementIndex);
                elementIndex += SIZE_OF_DOUBLE;
            }
        }
    }

    private static final Unsafe unsafe;

    static {
        try {
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }

            // make sure the VM thinks bytes are only one byte wide
            if (ARRAY_BYTE_INDEX_SCALE != 1) {
                throw new IllegalStateException("Byte array index scale must be 1, but is " + ARRAY_BYTE_INDEX_SCALE);
            }

            // make sure the VM thinks doubles are only eight byte wide
            if (ARRAY_DOUBLE_INDEX_SCALE != 8) {
                throw new IllegalStateException("Double array index scale must be 8, but is " + ARRAY_DOUBLE_INDEX_SCALE);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
