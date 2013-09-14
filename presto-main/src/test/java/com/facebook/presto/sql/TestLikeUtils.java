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
        Slice value = Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'});
        Regex regex = LikeUtils.likeToPattern("%b%", '\\');
        assertTrue(LikeUtils.regexMatches(regex, value));
    }

    @Test
    public void testBackslashesNoSpecialTreatment()
            throws Exception
    {
        Regex regex = LikeUtils.likeToPattern("\\abc\\/\\\\");
        assertTrue(LikeUtils.regexMatches(regex, utf8Slice("\\abc\\/\\\\")));
    }

    @Test
    public void testSelfEscaping()
            throws Exception
    {
        Regex regex = LikeUtils.likeToPattern("\\\\abc\\%", '\\');
        assertTrue(LikeUtils.regexMatches(regex, utf8Slice("\\abc%")));
    }

    @Test
    public void testAlternateEscapedCharacters()
            throws Exception
    {
        Regex regex = LikeUtils.likeToPattern("xxx%x_xabcxx", 'x');
        assertTrue(LikeUtils.regexMatches(regex, utf8Slice("x%_abcx")));
    }
}
