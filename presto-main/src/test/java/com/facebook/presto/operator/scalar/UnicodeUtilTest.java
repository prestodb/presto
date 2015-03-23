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

    @Test
    public void testCodePointCount() {
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("")), 0);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("hello")), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("Quadratically")), 13);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("\u00D6sterreich")), 10);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("Duli\u00F6 duli\u00F6")), 11);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B")), 7);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("na\u00EFve")), 5);
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("\uD801\uDC2D")), 1);
    }

    @Test
    public void testOffsetOfCodePointPosition() {
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice(""), 0), 0);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("hello"), 0), 0);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("hello"), 3), 3);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("hello"), 5), 5);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Quadratically"), 0), 0);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Quadratically"), 4), 4);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Quadratically"), 12), 12);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Quadratically"), 13), 13);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u00D6sterreich"), 0), 0);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u00D6sterreich"), 1), 2);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u00D6sterreich"), 7), 8);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Duli\u00F6 duli\u00F6"), 1), 1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Duli\u00F6 duli\u00F6"), 4), 4);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Duli\u00F6 duli\u00F6"), 5), 6);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Duli\u00F6 duli\u00F6"), 11), 13);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B"), 1), 3);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B"), 2), 6);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("na\u00EFve"), 3), 4);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\uD801\uDC2D"), 0), 0);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\uD801\uDC2D"), 1), 4);
    }

    @Test
    public void testOffsetOfCodePointPositionBounds() {
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice(""), 1), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("hello"), 6), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Quadratically"), 14), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u00D6sterreich"), 11), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("Duli\u00F6 duli\u00F6"), 12), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B"), 8), -1);
        assertEquals(UnicodeUtil.findUtf8OffsetOfCodePointPosition(Slices.utf8Slice("\uD801\uDC2D"), 2), -1);
    }
}