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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestSubfieldPathTokenizer
{
    @Test
    public void test()
    {
        List<SubfieldPath.PathElement> elements = ImmutableList.of(
                new SubfieldPath.NestedField("b"),
                new SubfieldPath.LongSubscript(2),
                new SubfieldPath.StringSubscript("z"),
                SubfieldPath.allSubscripts(),
                new SubfieldPath.StringSubscript("34"),
                new SubfieldPath.StringSubscript("b \"test\""),
                new SubfieldPath.StringSubscript("\"abc"),
                new SubfieldPath.StringSubscript("abc\""),
                new SubfieldPath.StringSubscript("ab\"cde"));

        for (SubfieldPath.PathElement element : elements) {
            assertPath(new SubfieldPath(ImmutableList.of(new SubfieldPath.NestedField("a"), element)));
        }

        for (SubfieldPath.PathElement element : elements) {
            for (SubfieldPath.PathElement secondElement : elements) {
                assertPath(new SubfieldPath(ImmutableList.of(new SubfieldPath.NestedField("a"), element, secondElement)));
            }
        }

        for (SubfieldPath.PathElement element : elements) {
            for (SubfieldPath.PathElement secondElement : elements) {
                for (SubfieldPath.PathElement thirdElement : elements) {
                    assertPath(new SubfieldPath(ImmutableList.of(new SubfieldPath.NestedField("a"), element, secondElement, thirdElement)));
                }
            }
        }
    }

    private static void assertPath(SubfieldPath path)
    {
        assertEquals(new SubfieldPath(Streams.stream(new SubfieldPathTokenizer(path.getPath())).collect(toImmutableList())), path);
    }
}
