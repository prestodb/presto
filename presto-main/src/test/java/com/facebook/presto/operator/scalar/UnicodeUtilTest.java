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
}