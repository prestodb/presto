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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class UnicodeUtilTest
{
    private static final String STRING_EMPTY = "";
    private static final String STRING_HELLO = "hello";
    private static final String STRING_QUADRATICALLY = "Quadratically";
    private static final String STRING_OESTERREICH = "\u00D6sterreich";
    private static final String STRING_DULIOE_DULIOE = "Duli\u00F6 duli\u00F6";
    private static final String STRING_FAITH_HOPE_LOVE = "\u4FE1\u5FF5,\u7231,\u5E0C\u671B";
    private static final String STRING_NAIVE = "na\u00EFve";
    private static final String STRING_OO = "\uD801\uDC2Dend";

    private static final byte[] INVALID_UTF8_1 = new byte[] {-127};
    private static final byte[] INVALID_UTF8_2 = new byte[] {50, -127, 52, 50};

    @Test
    public void testCodePointCount()
    {
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_EMPTY)), 0);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_HELLO)), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_QUADRATICALLY)), 13);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_OESTERREICH)), 10);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_DULIOE_DULIOE)), 11);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE)), 7);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_NAIVE)), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(STRING_OO)), 4);
    }

    @Test
    public void testIndexOfCodePointPosition()
    {
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_HELLO), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_HELLO), 3), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_QUADRATICALLY), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_QUADRATICALLY), 4), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_QUADRATICALLY), 12), 12);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OESTERREICH), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OESTERREICH), 1), 2);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OESTERREICH), 4), 5);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OESTERREICH), 7), 8);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 1), 1);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 4), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 5), 6);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 8), 9);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 1), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 2), 6);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_NAIVE), 3), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OO), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OO), 1), 4);
    }

    @Test
    public void testIndexOfCodePointPositionAtAndBeyondEnd()
    {
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_EMPTY), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_EMPTY), 1), 0);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_HELLO), 5), 5);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_HELLO), 6), 5);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_QUADRATICALLY), 13), 13);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_QUADRATICALLY), 14), 13);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OESTERREICH), 11), 11);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 11), 13);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_DULIOE_DULIOE), 12), 13);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 7), 17);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 8), 17);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OO), 4), 7);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(STRING_OO), 5), 7);
    }

    /**
     * Test invalid UTF8 encodings. We do not expect a 'correct' but none harmful result.
     */
    @Test
    public void testInvalidUtf8()
    {
        assertEquals(UnicodeUtil.countCodePoints(Slices.wrappedBuffer(INVALID_UTF8_1)), 0);
        assertEquals(UnicodeUtil.countCodePoints(Slices.wrappedBuffer(INVALID_UTF8_2)), 3);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_1), 0), 1);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_1), 1), 1);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_2), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_2), 1), 2);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_2), 2), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.wrappedBuffer(INVALID_UTF8_2), 3), 4);
    }

    @Test
    public void testFindUtf8IndexOfString()
    {
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 0, 13, Slices.utf8Slice("drat")), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 0, 7, Slices.utf8Slice("drat")), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 0, 6, Slices.utf8Slice("drat")), -1);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 0, 13, Slices.utf8Slice("lly")), 10);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 0, 13, Slices.utf8Slice("llyx")), -1);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_QUADRATICALLY), 11, 13, Slices.utf8Slice("qudra")), -1);

        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 0, 5, Slices.utf8Slice("hello")), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 0, 5, Slices.utf8Slice("o")), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 2, 5, Slices.utf8Slice("l")), 2);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 3, 5, Slices.utf8Slice("l")), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 4, 5, Slices.utf8Slice("l")), -1);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 4, 5, Slices.utf8Slice("ll")), -1);

        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_HELLO), 0, 5, Slices.utf8Slice(STRING_EMPTY)), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_EMPTY), 0, 5, Slices.utf8Slice(STRING_EMPTY)), 0);

        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 17, Slices.utf8Slice("\u7231")), 7);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 7, Slices.utf8Slice("\u7231")), -1);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 17, Slices.utf8Slice(",")), 6);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 17, Slices.utf8Slice("\u5E0C\u671B")), 11);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 14, Slices.utf8Slice("\u5E0C\u671B")), -1);
        assertEquals(UnicodeUtil.findUtf8IndexOfString(Slices.utf8Slice(STRING_FAITH_HOPE_LOVE), 0, 17, Slices.utf8Slice("\u5E0C\u671Bx")), -1);
    }

    @Test
    public void testLengthOfCodePoint()
    {
        assertEquals(UnicodeUtil.lengthOfCodePoint(0x24), 1);
        assertEquals(UnicodeUtil.lengthOfCodePoint(0xC2), 2);
        assertEquals(UnicodeUtil.lengthOfCodePoint(0xE2), 3);
        assertEquals(UnicodeUtil.lengthOfCodePoint(0xF0), 4);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testLengthOfCodePointIllegal1()
    {
        UnicodeUtil.lengthOfCodePoint(0x80);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testLengthOfCodePointIllegal2()
    {
        UnicodeUtil.lengthOfCodePoint(0xFE);
    }

    @Test
    public void testIsEmpty()
    {
        assertEquals(UnicodeUtil.isEmpty(Slices.EMPTY_SLICE), true);
        assertEquals(UnicodeUtil.isEmpty(Slices.utf8Slice("X")), false);
        assertEquals(UnicodeUtil.isEmpty(Slices.utf8Slice(STRING_OO)), false);
        assertEquals(UnicodeUtil.isEmpty(Slices.wrappedBuffer(INVALID_UTF8_1)), false);
    }
}
