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
package com.facebook.presto.spi;

import com.facebook.presto.spi.Subfield.NestedField;
import com.facebook.presto.spi.Subfield.PathElement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSubfieldTokenizer
{
    @Test
    public void test()
    {
        List<PathElement> elements = ImmutableList.of(
                new NestedField("b"),
                new NestedField("$bucket"),
                new Subfield.LongSubscript(2),
                new Subfield.StringSubscript("z"),
                Subfield.allSubscripts(),
                new Subfield.StringSubscript("34"),
                new Subfield.StringSubscript("b \"test\""),
                new Subfield.StringSubscript("\"abc"),
                new Subfield.StringSubscript("abc\""),
                new Subfield.StringSubscript("ab\"cde"),
                new Subfield.StringSubscript("a.b[\"hello\uDBFF\"]"));

        for (PathElement element : elements) {
            assertPath(new Subfield("a", ImmutableList.of(element)));
        }

        for (PathElement element : elements) {
            for (PathElement secondElement : elements) {
                assertPath(new Subfield("a", ImmutableList.of(element, secondElement)));
            }
        }

        for (PathElement element : elements) {
            for (PathElement secondElement : elements) {
                for (PathElement thirdElement : elements) {
                    assertPath(new Subfield("a", ImmutableList.of(element, secondElement, thirdElement)));
                }
            }
        }
    }

    private static void assertPath(Subfield path)
    {
        SubfieldTokenizer tokenizer = new SubfieldTokenizer(path.serialize());
        assertTrue(tokenizer.hasNext());
        assertEquals(new Subfield(((NestedField) tokenizer.next()).getName(), Streams.stream(tokenizer).collect(toImmutableList())), path);
    }

    @Test
    public void testInvalidPaths()
    {
        assertInvalidPath("a[b]");
        assertInvalidPath("a[2");
        assertInvalidPath("a.*");
        assertInvalidPath("a[2].[3].");
    }

    private void assertInvalidPath(String path)
    {
        SubfieldTokenizer tokenizer = new SubfieldTokenizer(path);

        try {
            Streams.stream(tokenizer).collect(toImmutableList());
            fail("Expected failure");
        }
        catch (PrestoException e) {
            // this is expected
            assertTrue(e.getMessage().startsWith("Invalid subfield path: "));
        }
    }
}
