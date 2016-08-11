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
package com.facebook.presto.server;

import io.airlift.slice.SliceInput;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestInputStreamSliceInputAdapter
{
    @Test
    public void testEmptyInput()
            throws Exception
    {
        SliceInput input = buildSliceInput(new byte[0]);
        assertEquals(input.position(), 0);
    }

    @Test
    public void testEmptyRead()
            throws Exception
    {
        SliceInput input = buildSliceInput(new byte[0]);
        assertEquals(input.read(), -1);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testEmptyReadByte()
            throws Exception
    {
        SliceInput input = buildSliceInput(new byte[0]);
        input.readByte();
    }

    @Test
    public void testEncodingBoolean()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {1}).readBoolean(), true);
        assertEquals(buildSliceInput(new byte[] {0}).readBoolean(), false);
    }

    @Test
    public void testEncodingByte()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {92}).readByte(), 92);
        assertEquals(buildSliceInput(new byte[] {-100}).readByte(), -100);
        assertEquals(buildSliceInput(new byte[] {-17}).readByte(), -17);
    }

    @Test
    public void testEncodingShort()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {109, 92}).readShort(), 23661);
        assertEquals(buildSliceInput(new byte[] {109, -100}).readShort(), -25491);
        assertEquals(buildSliceInput(new byte[] {-52, -107}).readShort(), -27188);

        assertEquals(buildSliceInput(new byte[] {109, -100}).readUnsignedShort(), 40045);
        assertEquals(buildSliceInput(new byte[] {-52, -107}).readUnsignedShort(), 38348);
    }

    @Test
    public void testEncodingInteger()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {109, 92, 75, 58}).readInt(), 978017389);
        assertEquals(buildSliceInput(new byte[] {-16, -60, -120, -1}).readInt(), -7813904);
    }

    @Test
    public void testEncodingLong()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {109, 92, 75, 58, 18, 120, -112, -17}).readLong(), -1184314682315678611L);
    }

    @Test
    public void testEncodingDouble()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {31, -123, -21, 81, -72, 30, 9, 64}).readDouble(), 3.14);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -8, 127}).readDouble(), Double.NaN);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, -1}).readDouble(), Double.NEGATIVE_INFINITY);
        assertEquals(buildSliceInput(new byte[] {0, 0, 0, 0, 0, 0, -16, 127}).readDouble(), Double.POSITIVE_INFINITY);
    }

    @Test
    public void testEncodingFloat()
            throws Exception
    {
        assertEquals(buildSliceInput(new byte[] {-61, -11, 72, 64}).readFloat(), 3.14f);
        assertEquals(buildSliceInput(new byte[] {0, 0, -64, 127}).readFloat(), Float.NaN);
        assertEquals(buildSliceInput(new byte[] {0, 0, -128, -1}).readFloat(), Float.NEGATIVE_INFINITY);
        assertEquals(buildSliceInput(new byte[] {0, 0, -128, 127}).readFloat(), Float.POSITIVE_INFINITY);
    }

    private SliceInput buildSliceInput(byte[] bytes)
    {
        FastByteArrayInputStream inputStream = new FastByteArrayInputStream(bytes);
        return new InputStreamSliceInputAdapter(inputStream, 16 * 1024);
    }
}
