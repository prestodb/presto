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
        assertEquals(UnicodeUtil.countCodePoints(Slices.utf8Slice("\u1042D")), 1);
    }
}