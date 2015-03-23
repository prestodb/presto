/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class UnicodeUtilTest {

    private static final String EMPTY = "";
    private static final String HELLO = "hello";
    private static final String QUADRATICALLY = "Quadratically";
    private static final String OESTERREICH = "\u00D6sterreich";
    private static final String DULIOE_DULIOE = "Duli\u00F6 duli\u00F6";
    private static final String FAITH_HOPE_LOVE = "\u4FE1\u5FF5,\u7231,\u5E0C\u671B";
    private static final String NAIVE = "na\u00EFve";
    private static final String OO = "\uD801\uDC2Dend";

    @Test
    public void testCodePointCount() {
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(EMPTY)), 0);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(HELLO)), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(QUADRATICALLY)), 13);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(OESTERREICH)), 10);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(DULIOE_DULIOE)), 11);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(FAITH_HOPE_LOVE)), 7);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(NAIVE)), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice(OO)), 4);
    }

    @Test
    public void testIndexOfCodePointPosition() {
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(HELLO), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(HELLO), 3), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(QUADRATICALLY), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(QUADRATICALLY), 4), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(QUADRATICALLY), 12), 12);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OESTERREICH), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OESTERREICH), 1), 2);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OESTERREICH), 4), 5);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OESTERREICH), 7), 8);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 1), 1);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 4), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 5), 6);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 8), 9);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(FAITH_HOPE_LOVE), 1), 3);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(FAITH_HOPE_LOVE), 2), 6);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(NAIVE), 3), 4);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OO), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OO), 1), 4);
    }

    @Test
    public void testIndexOfCodePointPositionAtAndBeyondEnd() {
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(EMPTY), 0), 0);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(EMPTY), 1), 0);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(HELLO), 5), 5);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(HELLO), 6), 5);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(QUADRATICALLY), 13), 13);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(QUADRATICALLY), 14), 13);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OESTERREICH), 11), 11);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 11), 13);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(DULIOE_DULIOE), 12), 13);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(FAITH_HOPE_LOVE), 7), 17);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(FAITH_HOPE_LOVE), 8), 17);

        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OO), 4), 7);
        assertEquals(UnicodeUtil.findUtf8IndexOfCodePointPosition(Slices.utf8Slice(OO), 5), 7);
    }
}