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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch;

import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.BitSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBitmap
{
    private TestBitmap() {}

    // The following byte arrays are arbitrary, of length 100
    private static final byte[] BYTE_STRING_A = new byte[] {
            44, -25, -97, 103, -34, 109, -83, -81, 105, 0, -36, -67, -42, 99, 43, -96, -34, -73, 31, 50,
            -18, -72, 73, -79, 74, 92, -15, -30, 61, -10, 17, 58, 24, -88, -104, 64, -99, 3, 85, 32,
            71, -10, 113, -126, -23, -101, 80, -7, -12, -43, -125, -63, 68, 93, -123, 26, 80, 86, 60, -1,
            64, -14, -100, 24, 86, -113, -76, 20, -62, -49, 25, -1, -59, 11, -8, -18, 43, 106, -23, -35,
            33, -55, 62, 77, -67, -11, 95, -72, -109, 17, -96, -76, -96, -82, 17, -98, 79, -123, 52, 41
    };
    private static final byte[] BYTE_STRING_B = new byte[] {
            -74, -71, 15, -96, 63, 53, 85, 95, -17, 109, 15, -41, 72, 3, -59, 74, -52, 123, 103, -110,
            -83, -128, 97, -5, -117, 61, -106, -17, -17, -106, 97, -85, 51, -101, -103, -82, 69, 103, 5, 4,
            -59, 21, -62, 101, -104, -43, 25, -103, -32, -20, 123, -37, -84, -55, -63, 31, -92, 13, -83, 31,
            -78, -8, 49, 124, 12, -52, -91, -41, 30, -105, 33, 42, -120, 67, -113, -115, -28, -101, 24, -77,
            -69, -71, -79, -1, 71, 88, 4, -21, -32, 43, 45, 100, 42, 37, 80, 49, -12, 39, -48, -4
    };

    @Test
    public static void testRoundTripFullLength()
    {
        // Since the last byte in BYTE_STRING_A is non-empty,
        // the Bitmap round-trip should exactly preserve the bytes (compare to testRoundTripTruncated)
        Bitmap bitmap = Bitmap.fromBytes(100 * 8, BYTE_STRING_A);
        assertEquals(bitmap.toBytes(), BYTE_STRING_A);
    }

    @Test
    public static void testRoundTripTruncated()
    {
        // Of the 100 bytes in BYTE_STRING_A, we force the 95-th to be the last non-empty byte
        // Because Bitmap does not serialize trailing zero bytes, only the first 95 bytes will survive the round-trip
        byte[] bytes = Arrays.copyOf(BYTE_STRING_A, 100);
        bytes[95] = 0;
        bytes[96] = 0;
        bytes[97] = 0;
        bytes[98] = 0;
        bytes[99] = 0;

        byte[] expectedBytes = Arrays.copyOf(bytes, 95);
        assertEquals(Bitmap.fromBytes(100 * 8, bytes).toBytes(), expectedBytes);
    }

    @Test
    public static void testSetBit()
    {
        Bitmap bitmap = new Bitmap(24);

        // This should create the following bitmap:
        // 00000011_00000101_01010101
        bitmap.setBit(0, true);
        bitmap.setBit(1, true);
        bitmap.setBit(8, true);
        bitmap.setBit(10, true);
        bitmap.setBit(16, true);
        bitmap.setBit(18, true);
        bitmap.setBit(20, true);
        bitmap.setBit(22, true);

        byte[] bytes = bitmap.toBytes();
        assertEquals(bytes[0], 0b00000011);
        assertEquals(bytes[1], 0b00000101);
        assertEquals(bytes[2], 0b01010101);

        // Now clear bits in positions 10+
        // This bitmap should now be:
        // 00000011_00000001 [_00000000] (the last byte will be truncated in toBytes())
        for (int i = 10; i < 24; i++) {
            bitmap.setBit(i, false);
        }

        bytes = bitmap.toBytes();
        assertEquals(bytes.length, 2);
        assertEquals(bytes[0], 0b00000011);
        assertEquals(bytes[1], 0b00000001);
    }

    @Test
    public static void testGetBit()
    {
        Bitmap bitmap = new Bitmap(4096);

        for (int i = 0; i < 4096; i++) {
            bitmap.setBit(i, true);
            assertTrue(bitmap.getBit(i));
            bitmap.setBit(i, false);
            assertFalse(bitmap.getBit(i));
        }
    }

    @Test
    public void testGetBitCount()
    {
        int length = 1024;
        Bitmap bitmap = new Bitmap(length);
        assertEquals(bitmap.getBitCount(), 0); // all zeros at initialization
        for (int i = 0; i < length; i++) {
            bitmap.setBit(i, true);
            assertEquals(bitmap.getBitCount(), i + 1); // i + 1 "true" bits
        }
    }

    @Test
    public static void testFlipBit()
    {
        Bitmap bitmap = new Bitmap(4096);

        for (int i = 0; i < 4096; i++) {
            bitmap.flipBit(i);
            assertTrue(bitmap.getBit(i));
            bitmap.flipBit(i);
            assertFalse(bitmap.getBit(i));
            bitmap.flipBit(i);
            assertTrue(bitmap.getBit(i));
        }
    }

    @Test
    public static void testByteLength()
    {
        for (int length : new int[] {8, 800}) {
            Bitmap bitmap = new Bitmap(length);
            for (int i = 0; i < length; i++) {
                bitmap.setBit(i, true);
                assertEquals(bitmap.byteLength(), bitmap.toBytes().length);
            }
        }
    }

    @Test
    public static void testLength()
    {
        for (int i = 1; i <= 10; i++) {
            Bitmap bitmap = new Bitmap(i * 8);
            assertEquals(bitmap.length(), i * 8);
        }
    }

    @Test
    public static void testRandomFlips()
    {
        Bitmap bitmap = new Bitmap(16);

        // Note: TestingDeterministicRandomizationStrategy flips deterministically if and only if probability >= 0.5.

        TestingDeterministicRandomizationStrategy randomizationStrategy = new TestingDeterministicRandomizationStrategy();
        bitmap.flipBit(0, 0.75, randomizationStrategy);
        assertTrue(bitmap.getBit(0));
        bitmap.flipBit(0, 0.75, randomizationStrategy);
        assertFalse(bitmap.getBit(0));
        bitmap.flipBit(0, 0.25, randomizationStrategy);
        assertFalse(bitmap.getBit(0));

        bitmap.flipAll(0.75, randomizationStrategy);
        for (int i = 0; i < 16; i++) {
            assertTrue(bitmap.getBit(i));
        }

        bitmap.flipAll(0.25, randomizationStrategy);
        for (int i = 0; i < 16; i++) {
            assertTrue(bitmap.getBit(i));
        }
    }

    @Test
    public static void testClone()
    {
        Bitmap bitmapA = Bitmap.fromBytes(100 * 8, BYTE_STRING_A);
        Bitmap bitmapB = bitmapA.clone();

        // all bits should match
        for (int i = 0; i < 100 * 8; i++) {
            assertEquals(bitmapA.getBit(i), bitmapB.getBit(i));
        }

        // but the bitmaps should point to different bits
        bitmapA.flipBit(0);
        assertEquals(bitmapA.getBit(0), !bitmapB.getBit(0));
    }

    @Test
    public static void testOr()
    {
        Bitmap bitmapA = Bitmap.fromBytes(100 * 8, BYTE_STRING_A);
        Bitmap bitmapB = Bitmap.fromBytes(100 * 8, BYTE_STRING_B);
        Bitmap bitmapC = bitmapA.clone();
        bitmapC.or(bitmapB);

        for (int i = 0; i < 100 * 8; i++) {
            assertEquals(bitmapC.getBit(i), bitmapA.getBit(i) | bitmapB.getBit(i));
        }
    }

    @Test
    public static void testXor()
    {
        Bitmap bitmapA = Bitmap.fromBytes(100 * 8, BYTE_STRING_A);
        Bitmap bitmapB = Bitmap.fromBytes(100 * 8, BYTE_STRING_B);
        Bitmap bitmapC = bitmapA.clone();
        bitmapC.xor(bitmapB);

        for (int i = 0; i < 100 * 8; i++) {
            assertEquals(bitmapC.getBit(i), bitmapA.getBit(i) ^ bitmapB.getBit(i));
        }
    }

    @Test
    public static void testRetainedSize()
    {
        int instanceSizes = ClassLayout.parseClass(Bitmap.class).instanceSize() + ClassLayout.parseClass(BitSet.class).instanceSize();

        // The underlying BitSet stores a long[] array of size length / 64,
        // even though toBytes() returns a truncated array of bytes.
        Bitmap bitmap = new Bitmap(1024);
        assertEquals(bitmap.getRetainedSizeInBytes(), instanceSizes + SizeOf.sizeOfLongArray(1024 / 64));
    }
}
