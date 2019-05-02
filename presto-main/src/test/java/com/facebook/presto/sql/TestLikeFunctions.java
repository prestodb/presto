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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.LikeFunctions.castCharToLikePattern;
import static com.facebook.presto.type.LikeFunctions.isLikePattern;
import static com.facebook.presto.type.LikeFunctions.likeChar;
import static com.facebook.presto.type.LikeFunctions.likePattern;
import static com.facebook.presto.type.LikeFunctions.likeVarchar;
import static com.facebook.presto.type.LikeFunctions.unescapeLiteralLikePattern;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestLikeFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testLikeBasic()
    {
        Regex regex = likePattern(utf8Slice("f%b__"));
        assertTrue(likeVarchar(utf8Slice("foobar"), regex));

        assertFunction("'foob' LIKE 'f%b__'", BOOLEAN, false);
        assertFunction("'foob' LIKE 'f%b'", BOOLEAN, true);
    }

    @Test
    public void testLikeChar()
    {
        Regex regex = likePattern(utf8Slice("f%b__"));
        assertTrue(likeChar(6L, utf8Slice("foobar"), regex));
        assertTrue(likeChar(6L, utf8Slice("foob"), regex));
        assertFalse(likeChar(7L, utf8Slice("foob"), regex));

        assertFunction("cast('foob' as char(6)) LIKE 'f%b__'", BOOLEAN, true);
        assertFunction("cast('foob' as char(7)) LIKE 'f%b__'", BOOLEAN, false);
    }

    @Test
    public void testLikeSpacesInPattern()
    {
        Regex regex = likePattern(utf8Slice("ala  "));
        assertTrue(likeVarchar(utf8Slice("ala  "), regex));
        assertFalse(likeVarchar(utf8Slice("ala"), regex));

        regex = castCharToLikePattern(5L, utf8Slice("ala"));
        assertTrue(likeVarchar(utf8Slice("ala  "), regex));
        assertFalse(likeVarchar(utf8Slice("ala"), regex));
    }

    @Test
    public void testLikeNewlineInPattern()
    {
        Regex regex = likePattern(utf8Slice("%o\nbar"));
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), regex));
    }

    @Test
    public void testLikeNewlineBeforeMatch()
    {
        Regex regex = likePattern(utf8Slice("%b%"));
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), regex));
    }

    @Test
    public void testLikeNewlineInMatch()
    {
        Regex regex = likePattern(utf8Slice("f%b%"));
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), regex));
    }

    @Test(timeOut = 1000)
    public void testLikeUtf8Pattern()
    {
        Regex regex = likePattern(utf8Slice("%\u540d\u8a89%"), utf8Slice("\\"));
        assertFalse(likeVarchar(utf8Slice("foo"), regex));
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Test(timeOut = 1000)
    public void testLikeInvalidUtf8Value()
    {
        Slice value = Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'});
        Regex regex = likePattern(utf8Slice("%b%"), utf8Slice("\\"));
        assertTrue(likeVarchar(value, regex));
    }

    @Test
    public void testBackslashesNoSpecialTreatment()
    {
        Regex regex = likePattern(utf8Slice("\\abc\\/\\\\"));
        assertTrue(likeVarchar(utf8Slice("\\abc\\/\\\\"), regex));
    }

    @Test
    public void testSelfEscaping()
    {
        Regex regex = likePattern(utf8Slice("\\\\abc\\%"), utf8Slice("\\"));
        assertTrue(likeVarchar(utf8Slice("\\abc%"), regex));
    }

    @Test
    public void testAlternateEscapedCharacters()
    {
        Regex regex = likePattern(utf8Slice("xxx%x_abcxx"), utf8Slice("x"));
        assertTrue(likeVarchar(utf8Slice("x%_abcx"), regex));
    }

    @Test
    public void testInvalidLikePattern()
    {
        assertThrows(PrestoException.class, () -> likePattern(utf8Slice("#"), utf8Slice("#")));
        assertThrows(PrestoException.class, () -> likePattern(utf8Slice("abc#abc"), utf8Slice("#")));
        assertThrows(PrestoException.class, () -> likePattern(utf8Slice("abc#"), utf8Slice("#")));
    }

    @Test
    public void testIsLikePattern()
    {
        assertFalse(isLikePattern(utf8Slice("abc"), null));
        assertFalse(isLikePattern(utf8Slice("abc#_def"), utf8Slice("#")));
        assertFalse(isLikePattern(utf8Slice("abc##def"), utf8Slice("#")));
        assertFalse(isLikePattern(utf8Slice("abc#%def"), utf8Slice("#")));
        assertTrue(isLikePattern(utf8Slice("abc%def"), null));
        assertTrue(isLikePattern(utf8Slice("abcdef_"), null));
        assertTrue(isLikePattern(utf8Slice("abcdef##_"), utf8Slice("#")));
        assertTrue(isLikePattern(utf8Slice("%abcdef#_"), utf8Slice("#")));
        assertThrows(PrestoException.class, () -> isLikePattern(utf8Slice("#"), utf8Slice("#")));
        assertThrows(PrestoException.class, () -> isLikePattern(utf8Slice("abc#abc"), utf8Slice("#")));
        assertThrows(PrestoException.class, () -> isLikePattern(utf8Slice("abc#"), utf8Slice("#")));
    }

    @Test
    public void testUnescapeValidLikePattern()
    {
        assertEquals(unescapeLiteralLikePattern(utf8Slice("abc"), null), utf8Slice("abc"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("abc#_"), utf8Slice("#")), utf8Slice("abc_"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("a##bc#_"), utf8Slice("#")), utf8Slice("a#bc_"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("a###_bc"), utf8Slice("#")), utf8Slice("a#_bc"));
    }
}
