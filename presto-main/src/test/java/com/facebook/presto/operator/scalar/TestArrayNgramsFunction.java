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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
public class TestArrayNgramsFunction

        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 1)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar"),
                ImmutableList.of("foo"),
                ImmutableList.of("baz"),
                ImmutableList.of("foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo"),
                ImmutableList.of("foo", "baz"),
                ImmutableList.of("baz", "foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 3)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz"),
                ImmutableList.of("foo", "baz", "foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 4)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz", "foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 5)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz", "foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 6)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz", "foo")));
        assertFunction("ngrams(ARRAY['bar', 'foo', 'baz', 'foo'], 100000000)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("bar", "foo", "baz", "foo")));
        assertFunction("ngrams(ARRAY['a', 'bb', 'ccc', 'dddd'], 2)", new ArrayType(new ArrayType(createVarcharType(4))), ImmutableList.of(
                ImmutableList.of("a", "bb"),
                ImmutableList.of("bb", "ccc"),
                ImmutableList.of("ccc", "dddd")));
    }

    @Test
    public void testNull()
    {
        assertFunction("ngrams(ARRAY['foo', NULL, 'bar'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                asList("foo", null),
                asList(null, "bar")));

        assertFunction("ngrams(ARRAY [NULL, NULL, NULL], 2)", new ArrayType(new ArrayType(UNKNOWN)), ImmutableList.of(
                asList(null, null),
                asList(null, null)));

        assertFunction("ngrams(ARRAY [NULL, 3, NULL], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(
                asList(null, 3),
                asList(3, null)));
    }

    @Test
    public void testTypeCombinations()
    {
        assertFunction("ngrams(ARRAY[1, 2, 3], 2)", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(
                        ImmutableList.of(1, 2),
                        ImmutableList.of(2, 3)));
        assertFunction("ngrams(ARRAY[1.1E0, 2.1E0, 3.1E0], 2)", new ArrayType(new ArrayType(DOUBLE)), ImmutableList.of(
                ImmutableList.of(1.1, 2.1),
                ImmutableList.of(2.1, 3.1)));
        assertFunction("ngrams(ARRAY[true, false, true], 2)", new ArrayType(new ArrayType(BOOLEAN)), ImmutableList.of(
                ImmutableList.of(true, false),
                ImmutableList.of(false, true)));

        assertFunction("ngrams(ARRAY[ARRAY['A1', 'A2'], ARRAY['B1'], ARRAY['C1', 'C2']], 2)", new ArrayType(new ArrayType(new ArrayType(createVarcharType(2)))), ImmutableList.of(
                ImmutableList.of(ImmutableList.of("A1", "A2"), ImmutableList.of("B1")),
                ImmutableList.of(ImmutableList.of("B1"), ImmutableList.of("C1", "C2"))));

        assertFunction("ngrams(ARRAY['\u4FE1\u5FF5\u7231', '\u5E0C\u671B', '\u671B'], 2)", new ArrayType(new ArrayType(createVarcharType(3))), ImmutableList.of(
                ImmutableList.of("\u4FE1\u5FF5\u7231", "\u5E0C\u671B"),
                ImmutableList.of("\u5E0C\u671B", "\u671B")));

        assertFunction("ngrams(ARRAY[], 2)", new ArrayType(new ArrayType(UNKNOWN)), ImmutableList.of(
                asList()));
        assertFunction("ngrams(ARRAY[''], 2)", new ArrayType(new ArrayType(createVarcharType(0))), ImmutableList.of(
                ImmutableList.of("")));
        assertFunction("ngrams(ARRAY['', ''], 2)", new ArrayType(new ArrayType(createVarcharType(0))), ImmutableList.of(
                ImmutableList.of("", "")));

        assertInvalidFunction("ngrams(ARRAY['foo','bar'], 0)", "N must be positive");
        assertInvalidFunction("ngrams(ARRAY['foo','bar'], 0)", "N must be positive");
    }
}
