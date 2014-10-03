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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.type.ArrayType.rawSlicesToStackRepresentation;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestArrayOperators
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testStackRepresentation()
            throws Exception
    {
        Slice slice = rawSlicesToStackRepresentation(ImmutableList.of(toStackRepresentation(ImmutableList.of(1L, 2L)), toStackRepresentation(ImmutableList.of(3L))));
        assertEquals(slice, Slices.utf8Slice("[[1,2],[3]]"));
    }

    @Test
    public void testArrayToJson()
            throws Exception
    {
        assertFunction("CAST(ARRAY [1, 2, 3] AS JSON)", "[1,2,3]");
        assertFunction("CAST(ARRAY [1, NULL, 3] AS JSON)", "[1,null,3]");
        assertFunction("CAST(ARRAY [1, 2.0, 3] AS JSON)", "[1.0,2.0,3.0]");
        assertFunction("CAST(ARRAY [1.0, 2.5, 3.0] AS JSON)", "[1.0,2.5,3.0]");
        assertFunction("CAST(ARRAY ['puppies', 'kittens'] AS JSON)", "[\"puppies\",\"kittens\"]");
        assertFunction("CAST(ARRAY [TRUE, FALSE] AS JSON)", "[true,false]");
        assertFunction("CAST(ARRAY [from_unixtime(1)] AS JSON)", "[\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()).toString() + "\"]");
    }

    @Test
    public void testConstructor()
            throws Exception
    {
        assertFunction("ARRAY []", ImmutableList.of());
        assertFunction("ARRAY [NULL]", Lists.newArrayList((Object) null));
        assertFunction("ARRAY [1, 2, 3]", ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY [1, NULL, 3]", Lists.newArrayList(1L, null, 3L));
        assertFunction("ARRAY [NULL, 2, 3]", Lists.newArrayList(null, 2L, 3L));
        assertFunction("ARRAY [1, 2.0, 3]", ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]]", ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L)));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]", Lists.newArrayList(ImmutableList.of(1L, 2L), null, ImmutableList.of(3L)));
        assertFunction("ARRAY [1.0, 2.5, 3.0]", ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("ARRAY ['puppies', 'kittens']", ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE, FALSE]", ImmutableList.of(true, false));
        assertFunction("ARRAY [from_unixtime(1), from_unixtime(100)]", ImmutableList.of(
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
    }

    @Test
    public void testArrayContains()
            throws Exception
    {
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 2)", true);
        assertFunction("CONTAINS(ARRAY [1, 2, 3], 5)", false);
        assertFunction("CONTAINS(ARRAY [1, NULL, 3], 1)", true);
        assertFunction("CONTAINS(ARRAY [1, 2.0, 3], 3.0)", true);
        assertFunction("CONTAINS(ARRAY [1.0, 2.5, 3.0], 2.2)", false);
        assertFunction("CONTAINS(ARRAY ['puppies', 'kittens'], 'kittens')", true);
        assertFunction("CONTAINS(ARRAY ['puppies', 'kittens'], 'lizards')", false);
        assertFunction("CONTAINS(ARRAY [TRUE, FALSE], TRUE)", true);
        assertFunction("CONTAINS(ARRAY [FALSE], TRUE)", false);
    }

    @Test
    public void testCardinality()
            throws Exception
    {
        assertFunction("CARDINALITY(ARRAY [])", 0);
        assertFunction("CARDINALITY(ARRAY [NULL])", 1);
        assertFunction("CARDINALITY(ARRAY [1, 2, 3])", 3);
        assertFunction("CARDINALITY(ARRAY [1, NULL, 3])", 3);
        assertFunction("CARDINALITY(ARRAY [1, 2.0, 3])", 3);
        assertFunction("CARDINALITY(ARRAY [ARRAY[1, 2], ARRAY[3]])", 2);
        assertFunction("CARDINALITY(ARRAY [1.0, 2.5, 3.0])", 3);
        assertFunction("CARDINALITY(ARRAY ['puppies', 'kittens'])", 2);
        assertFunction("CARDINALITY(ARRAY [TRUE, FALSE])", 2);
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        try {
            assertFunction("ARRAY [1, 2, 3][0]", null);
            fail("Access to array element zero should fail");
        }
        catch (PrestoException e) {
            // Expected
        }
        try {
            assertFunction("ARRAY [1, 2, 3][-1]", null);
            fail("Access to negative array element should fail");
        }
        catch (RuntimeException e) {
            // Expected
        }
        try {
            assertFunction("ARRAY [1, 2, 3][4]", null);
            fail("Access to out of bounds array element should fail");
        }
        catch (RuntimeException e) {
            // Expected
        }
        try {
            assertFunction("ARRAY [1, 2, 3][1.1]", null);
            fail("Access to array with double subscript should fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getCode() == SemanticErrorCode.TYPE_MISMATCH);
        }
        assertFunction("1 + ARRAY [2, 1, 3][2]", 2);
        assertFunction("ARRAY [2, 1, 3][2]", 1);
        assertFunction("ARRAY [2, NULL, 3][2]", null);
        assertFunction("ARRAY [1.0, 2.5, 3.5][3]", 3.5);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2]", ImmutableList.of(3L));
        assertFunction("ARRAY [ARRAY[1, 2], NULL, ARRAY[3]][2]", null);
        assertFunction("ARRAY [ARRAY[1, 2], ARRAY[3]][2][1]", 3);
        assertFunction("ARRAY ['puppies', 'kittens'][2]", "kittens");
        assertFunction("ARRAY ['puppies', 'kittens', NULL][3]", null);
        assertFunction("ARRAY [TRUE, FALSE][2]", false);
        assertFunction("ARRAY [from_unixtime(1), from_unixtime(100)][1]", new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()));
    }
}
