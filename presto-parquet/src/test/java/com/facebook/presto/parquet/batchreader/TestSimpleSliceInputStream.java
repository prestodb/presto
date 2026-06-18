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
package com.facebook.presto.parquet.batchreader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSimpleSliceInputStream
{
    @DataProvider(name = "dataTypes")
    public Object[][] dataTypes()
    {
        return new Object[][] {
                {Byte.TYPE, Byte.BYTES},
                {Short.TYPE, Short.BYTES},
                {Integer.TYPE, Integer.BYTES},
                {Long.TYPE, Long.BYTES},
        };
    }

    @Test(dataProvider = "dataTypes")
    public void testReadSimpleType(Class<Object> clazz, int sizeOfType)
    {
        int numElements = 100;
        Slice slice = Slices.allocate(sizeOfType * numElements);
        for (int i = 0; i < numElements; i++) {
            switch (clazz.getName()) {
                case "byte":
                    slice.setByte(i, i);
                    break;
                case "short":
                    slice.setShort(i * sizeOfType, i);
                    break;
                case "int":
                    slice.setInt(i * sizeOfType, i);
                    break;
                case "long":
                    slice.setLong(i * sizeOfType, i);
                    break;
                default:
            }
        }

        SimpleSliceInputStream simpleSliceInputStream = new SimpleSliceInputStream(slice);
        for (int i = 0; i < numElements; i++) {
            switch (clazz.getName()) {
                case "byte":
                    byte actualByte = simpleSliceInputStream.readByte();
                    assertEquals(actualByte, i);
                    break;
                case "short":
                    short actualShort = simpleSliceInputStream.readShort();
                    assertEquals(actualShort, i);
                    break;
                case "int":
                    int actualInt = simpleSliceInputStream.readInt();
                    assertEquals(actualInt, i);
                    break;
                case "long":
                    long actualLong = simpleSliceInputStream.readLong();
                    assertEquals(actualLong, i);
                    break;
                default:
            }
        }
    }

    @Test
    public void testReadBytes()
    {
        int numElements = 100;
        Slice slice = Slices.allocate(2 * numElements);
        byte[] expected = new byte[2 * numElements];

        int offset = 0;
        for (int i = 0; i < numElements; i++) {
            String str = "" + i;
            slice.setBytes(offset, str.getBytes());
            int length = str.getBytes().length;
            System.arraycopy(str.getBytes(), 0, expected, offset, length);
            offset += length;
        }

        SimpleSliceInputStream simpleSliceInputStream = new SimpleSliceInputStream(slice);
        byte[] actual = simpleSliceInputStream.readBytes();
        assertEquals(actual, expected);
    }

    @Test
    public void testReadMixData()
    {
        Slice slice = Slices.allocate(Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES);

        int offset = 0;

        // Write byte
        slice.setByte(offset, Byte.MAX_VALUE);
        offset += Byte.BYTES;

        // Write short
        slice.setShort(offset, Short.MAX_VALUE);
        offset += Short.BYTES;

        // Write int
        slice.setInt(offset, Integer.MAX_VALUE);
        offset += Integer.BYTES;

        // Write int
        slice.setLong(offset, Long.MAX_VALUE);

        SimpleSliceInputStream simpleSliceInputStream = new SimpleSliceInputStream(slice);
        byte actualByte = simpleSliceInputStream.readByte();
        assertEquals(actualByte, Byte.MAX_VALUE);

        short actualShort = simpleSliceInputStream.readShort();
        assertEquals(actualShort, Short.MAX_VALUE);

        int actualInt = simpleSliceInputStream.readInt();
        assertEquals(actualInt, Integer.MAX_VALUE);

        long actualLong = simpleSliceInputStream.readLong();
        assertEquals(actualLong, Long.MAX_VALUE);
    }

    @Test
    public void testGetByteArray()
    {
        int numElements = 100;
        Slice slice = Slices.allocate(2 * numElements);
        byte[] expected = new byte[2 * numElements];

        int offset = 0;
        for (int i = 0; i < numElements; i++) {
            String str = "" + i;
            slice.setBytes(offset, str.getBytes());
            int length = str.getBytes().length;
            System.arraycopy(str.getBytes(), 0, expected, offset, length);
            offset += length;
        }

        SimpleSliceInputStream simpleSliceInputStream = new SimpleSliceInputStream(slice);
        byte[] actual = simpleSliceInputStream.getByteArray();
        assertEquals(actual, expected);
    }
}
