package com.facebook.presto.sql;

import com.facebook.presto.sql.planner.LikeUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joni.Regex;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLikeUtils
{
    @Test(timeOut = 1000)
    public void testLikeUtf8Pattern()
    {
        Regex regex = LikeUtils.likeToPattern(utf8Slice("%\u540d\u8a89%"), utf8Slice("\\"));
        assertFalse(LikeUtils.regexMatches(regex, utf8Slice("foo")));
    }

    @Test(timeOut = 1000)
    public void testLikeInvalidUtf8Value()
    {
        Slice value = Slices.wrappedBuffer(new byte[] { 'a', 'b', 'c', (byte) 0xFF, 'x', 'y' });
        Regex regex = LikeUtils.likeToPattern("%b%", '\\');
        assertTrue(LikeUtils.regexMatches(regex, value));
    }
}
