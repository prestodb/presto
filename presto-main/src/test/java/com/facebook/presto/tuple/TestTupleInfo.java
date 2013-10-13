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
package com.facebook.presto.tuple;

import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.tuple.TupleInfo.Builder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleInfo
{
    @Test
    public void testSingleBooleanLength()
    {
        TupleInfo info = new TupleInfo(BOOLEAN);

        RandomAccessBlock value = info.builder()
                .append(true)
                .build();

        assertEquals(value.getBoolean(0), true);
        assertEquals(value.getObjectValue(0), true);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_BYTE + SIZE_OF_BYTE);

        value = info.builder()
                .append(false)
                .build();

        assertFalse(value.isNull(0));
        assertEquals(value.getBoolean(0), false);
        assertEquals(value.getObjectValue(0), false);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_BYTE + SIZE_OF_BYTE);
    }

    @Test
    public void testSingleBooleanLengthNull()
    {
        RandomAccessBlock value = TupleInfo.SINGLE_BOOLEAN.builder()
                .appendNull()
                .build();

        assertTrue(value.isNull(0));
        // value of a null boolean is false
        assertEquals(value.getBoolean(0), false);
        assertEquals(value.getObjectValue(0), null);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_BYTE + SIZE_OF_BYTE);
    }

    @Test
    public void testSingleLongLength()
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64);

        RandomAccessBlock value = info.builder()
                .append(42)
                .build();

        assertEquals(value.getLong(0), 42L);
        assertEquals(value.getObjectValue(0), 42L);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_LONG + SIZE_OF_BYTE);
    }

    @Test
    public void testSingleLongLengthNull()
    {
        RandomAccessBlock value = TupleInfo.SINGLE_LONG.builder()
                .appendNull()
                .build();

        assertTrue(value.isNull(0));
        // value of a null long is 0
        assertEquals(value.getLong(0), 0L);
        assertEquals(value.getObjectValue(0), null);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_LONG + SIZE_OF_BYTE);
    }

    @Test
    public void testAppendWithNull()
    {
        Builder builder = TupleInfo.SINGLE_LONG.builder();
        assertTrue(builder.appendNull().build().isNull(0));
    }

    @Test
    public void testSingleDoubleLength()
    {
        TupleInfo info = new TupleInfo(DOUBLE);

        RandomAccessBlock value = info.builder()
                .append(42.42)
                .build();

        assertEquals(value.getDouble(0), 42.42);
        assertEquals(value.getObjectValue(0), 42.42);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_DOUBLE + SIZE_OF_BYTE);
    }

    @Test
    public void testSingleDoubleLengthNull()
    {
        RandomAccessBlock value = TupleInfo.SINGLE_DOUBLE.builder()
                .appendNull()
                .build();

        assertTrue(value.isNull(0));
        // value of a null double is 0
        assertEquals(value.getDouble(0), 0.0);
        assertEquals(value.getObjectValue(0), null);
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_DOUBLE + SIZE_OF_BYTE);
    }

    @Test
    public void testSingleVariableLength()
    {
        Slice binary = Slices.wrappedBuffer(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        RandomAccessBlock value = TupleInfo.SINGLE_VARBINARY.builder()
                .append(binary)
                .build();

        assertEquals(value.getDataSize().toBytes(), binary.length() + SIZE_OF_INT + SIZE_OF_BYTE);
        assertEquals(value.getSlice(0), binary);
        assertEquals(value.getObjectValue(0), binary.toStringUtf8());
    }

    @Test
    public void testSingleVariableLengthNull()
    {
        RandomAccessBlock value = TupleInfo.SINGLE_VARBINARY.builder()
                .appendNull()
                .build();

        assertTrue(value.isNull(0));
        assertEquals(value.getDataSize().toBytes(), SIZE_OF_BYTE);

        // can not get slice of a null
        try {
            value.getSlice(0);
        }
        catch (IllegalStateException e) {
        }
        assertEquals(value.getObjectValue(0), null);
    }

    @Test
    public void testSetNullAtNonZeroOffset()
            throws Exception
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64);
        Slice slice = Slices.allocate(info.getFixedSize() * 2);

        info.setNull(slice, FIXED_INT_64.getSize());
        assertTrue(info.isNull(slice, FIXED_INT_64.getSize()));
    }

    @Test
    public void testSetNonNullAtNonZeroOffset()
            throws Exception
    {
        TupleInfo info = new TupleInfo(FIXED_INT_64);
        Slice slice = Slices.allocate(info.getFixedSize() * 2);

        // initialize to nulls
        info.setNull(slice, 0);
        assertTrue(info.isNull(slice, 0));

        info.setNull(slice, info.getFixedSize());
        assertTrue(info.isNull(slice, info.getFixedSize()));

        // now test setNotNull
        info.setNotNull(slice, info.getFixedSize());
        assertFalse(info.isNull(slice, info.getFixedSize()));
    }
}
