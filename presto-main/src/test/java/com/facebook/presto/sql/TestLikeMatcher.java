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

import com.facebook.presto.likematcher.LikeMatcher;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

public class TestLikeMatcher
{
    @Test
    public void test()
    {
        // min length short-circuit
        assertFalse(match("__", "a"));

        // max length short-circuit
        assertFalse(match("__", "abcdefghi"));

        // prefix short-circuit
        assertFalse(match("a%", "xyz"));

        // prefix match
        assertTrue(match("a%", "a"));
        assertTrue(match("a%", "ab"));
        assertTrue(match("a_", "ab"));

        // suffix short-circuit
        assertFalse(match("%a", "xyz"));

        // suffix match
        assertTrue(match("%z", "z"));
        assertTrue(match("%z", "yz"));
        assertTrue(match("_z", "yz"));

        // match literal
        assertTrue(match("abcd", "abcd"));

        // match one
        assertFalse(match("_", ""));
        assertTrue(match("_", "a"));
        assertFalse(match("_", "ab"));

        // match zero or more
        assertTrue(match("%", ""));
        assertTrue(match("%", "a"));
        assertTrue(match("%", "ab"));

        // non-strict matching
        assertTrue(match("_%", "abcdefg"));
        assertFalse(match("_a%", "abcdefg"));

        // strict matching
        assertTrue(match("_ab_", "xabc"));
        assertFalse(match("_ab_", "xyxw"));
        assertTrue(match("_a%b_", "xaxxxbx"));

        // optimization of consecutive _ and %
        assertTrue(match("_%_%_%_%", "abcdefghij"));

        // utf-8
        LikeMatcher single = LikeMatcher.compile("_");
        LikeMatcher multiple = LikeMatcher.compile("_a%b_"); // prefix and suffix with _a and b_ to avoid optimizations
        for (int i = 0; i < Character.MAX_CODE_POINT; i++) {
            assertTrue(single.match(Character.toString(i).getBytes(StandardCharsets.UTF_8)));

            String value = "aa" + (char) i + "bb";
            assertTrue(multiple.match(value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void testEscape()
    {
        assertTrue(match("-%", "%", '-'));
        assertTrue(match("-_", "_", '-'));
        assertTrue(match("--", "-", '-'));
    }

    private static boolean match(String pattern, String value)
    {
        return match(pattern, value, Optional.empty());
    }

    private static boolean match(String pattern, String value, char escape)
    {
        return match(pattern, value, Optional.of(escape));
    }

    private static boolean match(String pattern, String value, Optional<Character> escape)
    {
        String padding = "++++";
        String padded = padding + value + padding;
        byte[] bytes = padded.getBytes(StandardCharsets.UTF_8);

        boolean withoutPadding = LikeMatcher.compile(pattern, escape).match(value.getBytes(StandardCharsets.UTF_8));
        boolean withPadding = LikeMatcher.compile(pattern, escape).match(bytes, padding.length(), bytes.length - padding.length() * 2);  // exclude padding

        assertEquals(withoutPadding, withPadding);
        return withPadding;
    }
}
