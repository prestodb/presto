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

import com.facebook.presto.common.type.BooleanType;
import org.testng.annotations.Test;

public class TestArrayMatchFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testAllMatch()
    {
        assertFunction("all_match(ARRAY [5, 7, 9], x -> x % 2 = 1)", BooleanType.BOOLEAN, true);
        assertFunction("all_match(ARRAY [true, false, true], x -> x)", BooleanType.BOOLEAN, false);
        assertFunction("all_match(ARRAY ['abc', 'ade', 'afg'], x -> substr(x, 1, 1) = 'a')", BooleanType.BOOLEAN, true);
        assertFunction("all_match(ARRAY [], x -> true)", BooleanType.BOOLEAN, true);
        assertFunction("all_match(ARRAY [true, true, NULL], x -> x)", BooleanType.BOOLEAN, null);
        assertFunction("all_match(ARRAY [true, false, NULL], x -> x)", BooleanType.BOOLEAN, false);
        assertFunction("all_match(ARRAY [NULL, NULL, NULL], x -> x > 1)", BooleanType.BOOLEAN, null);
        assertFunction("all_match(ARRAY [NULL, NULL, NULL], x -> x IS NULL)", BooleanType.BOOLEAN, true);
        assertFunction("all_match(ARRAY [MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])], x -> cardinality(x) > 1)", BooleanType.BOOLEAN, true);
    }

    @Test
    public void testAnyMatch()
    {
        assertFunction("any_match(ARRAY [5, 8, 10], x -> x % 2 = 1)", BooleanType.BOOLEAN, true);
        assertFunction("any_match(ARRAY [false, false, false], x -> x)", BooleanType.BOOLEAN, false);
        assertFunction("any_match(ARRAY ['abc', 'def', 'ghi'], x -> substr(x, 1, 1) = 'a')", BooleanType.BOOLEAN, true);
        assertFunction("any_match(ARRAY [], x -> true)", BooleanType.BOOLEAN, false);
        assertFunction("any_match(ARRAY [false, false, NULL], x -> x)", BooleanType.BOOLEAN, null);
        assertFunction("any_match(ARRAY [true, false, NULL], x -> x)", BooleanType.BOOLEAN, true);
        assertFunction("any_match(ARRAY [NULL, NULL, NULL], x -> x > 1)", BooleanType.BOOLEAN, null);
        assertFunction("any_match(ARRAY [true, false, NULL], x -> x IS NULL)", BooleanType.BOOLEAN, true);
        assertFunction("any_match(ARRAY [MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])], x -> cardinality(x) > 4)", BooleanType.BOOLEAN, false);
    }

    @Test
    public void testNoneMatch()
    {
        assertFunction("none_match(ARRAY [5, 8, 10], x -> x % 2 = 1)", BooleanType.BOOLEAN, false);
        assertFunction("none_match(ARRAY [false, false, false], x -> x)", BooleanType.BOOLEAN, true);
        assertFunction("none_match(ARRAY ['abc', 'def', 'ghi'], x -> substr(x, 1, 1) = 'a')", BooleanType.BOOLEAN, false);
        assertFunction("none_match(ARRAY [], x -> true)", BooleanType.BOOLEAN, true);
        assertFunction("none_match(ARRAY [false, false, NULL], x -> x)", BooleanType.BOOLEAN, null);
        assertFunction("none_match(ARRAY [true, false, NULL], x -> x)", BooleanType.BOOLEAN, false);
        assertFunction("none_match(ARRAY [NULL, NULL, NULL], x -> x > 1)", BooleanType.BOOLEAN, null);
        assertFunction("none_match(ARRAY [true, false, NULL], x -> x IS NULL)", BooleanType.BOOLEAN, false);
        assertFunction("none_match(ARRAY [MAP(ARRAY[1,2], ARRAY[3,4]), MAP(ARRAY[1,2,3], ARRAY[3,4,5])], x -> cardinality(x) > 4)", BooleanType.BOOLEAN, true);
    }
}
