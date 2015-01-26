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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.ArrayType.rawSlicesToStackRepresentation;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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
        functionAssertions.getMetadata().getFunctionRegistry().addFunctions(new FunctionListBuilder(functionAssertions.getMetadata().getTypeManager()).scalar(TestingRowConstructor.class).getFunctions());
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private void assertInvalidFunction(String projection, String message)
    {
        try {
            assertFunction(projection, null);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_FUNCTION_ARGUMENT.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }

    private void assertInvalidFunction(String projection)
    {
        functionAssertions.assertInvalidFunction(projection);
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
    public void testJsonToArray()
            throws Exception
    {
        assertFunction("CAST(CAST('[1, 2, 3]' AS JSON) AS ARRAY<BIGINT>)", ImmutableList.of(1L, 2L, 3L));
        assertFunction("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<BIGINT>)", asList(1L, null, 3L));
        assertFunction("CAST(CAST('[1, 2.0, 3]' AS JSON) AS ARRAY<DOUBLE>)", ImmutableList.of(1.0, 2.0, 3.0));
        assertFunction("CAST(CAST('[1.0, 2.5, 3.0]' AS JSON) AS ARRAY<DOUBLE>)", ImmutableList.of(1.0, 2.5, 3.0));
        assertFunction("CAST(CAST('[\"puppies\", \"kittens\"]' AS JSON) AS ARRAY<VARCHAR>)", ImmutableList.of("puppies", "kittens"));
        assertFunction("CAST(CAST('[true, false]' AS JSON) AS ARRAY<BOOLEAN>)", ImmutableList.of(true, false));
        assertFunction("CAST(CAST('[[1], [null]]' AS JSON) AS ARRAY<ARRAY<BIGINT>>)", asList(asList(1L), asList((Long) null)));
        assertFunction("CAST(CAST('null' AS JSON) AS ARRAY<BIGINT>)", null);
        assertInvalidFunction("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<TIMESTAMP>)");
        assertInvalidFunction("CAST(CAST('[1, null, 3]' AS JSON) AS ARRAY<ARRAY<TIMESTAMP>>)");
        assertInvalidFunction("CAST(CAST('[1, 2, 3]' AS JSON) AS ARRAY<BOOLEAN>)");
        assertInvalidFunction("CAST(CAST('[\"puppies\", \"kittens\"]' AS JSON) AS ARRAY<BIGINT>)");
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
        assertFunction("ARRAY [sqrt(-1)]", ImmutableList.of(NaN));
        assertFunction("ARRAY [pow(infinity(), 2)]", ImmutableList.of(POSITIVE_INFINITY));
        assertFunction("ARRAY [pow(-infinity(), 1)]", ImmutableList.of(NEGATIVE_INFINITY));
    }

    @Test
    public void testArrayToArrayConcat()
            throws Exception
    {
        assertFunction("ARRAY [1, NULL] || ARRAY [3]", Lists.newArrayList(1L, null, 3L));
        assertFunction("ARRAY [1, 2] || ARRAY[3, 4]", ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY [NULL] || ARRAY[NULL]", Lists.newArrayList(null, null));
        assertFunction("ARRAY ['puppies'] || ARRAY ['kittens']", ImmutableList.of("puppies", "kittens"));
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE]", ImmutableList.of(true, false));
        assertFunction("concat(ARRAY [1] , ARRAY[2,3])", ImmutableList.of(1L, 2L, 3L));
        assertFunction("ARRAY [from_unixtime(1)] || ARRAY[from_unixtime(100)]", ImmutableList.of(
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("ARRAY [ARRAY[ARRAY[1]]] || ARRAY [ARRAY[ARRAY[2]]]", asList(singletonList(Longs.asList(1)), singletonList(Longs.asList(2))));
        assertFunction("ARRAY [] || ARRAY []", ImmutableList.of());
        assertFunction("ARRAY [TRUE] || ARRAY [FALSE] || ARRAY [TRUE]", ImmutableList.of(true, false, true));
        assertFunction("ARRAY [1] || ARRAY [2] || ARRAY [3] || ARRAY [4]", ImmutableList.of(1L, 2L, 3L, 4L));

        try {
            assertFunction("ARRAY [ARRAY[1]] || ARRAY[ARRAY[true], ARRAY[false]]", null);
            fail("arrays must be of the same type");
        }
        catch (RuntimeException e) {
            // Expected
        }

        try {
            assertFunction("ARRAY [ARRAY[1]] || ARRAY[NULL]", null);
            fail("arrays must be of the same type");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testElementArrayConcat()
            throws Exception
    {
        assertFunction("CAST (ARRAY [DATE '2001-08-22'] || DATE '2001-08-23' AS JSON)", "[\"2001-08-22\",\"2001-08-23\"]");
        assertFunction("CAST (DATE '2001-08-23' || ARRAY [DATE '2001-08-22'] AS JSON)", "[\"2001-08-23\",\"2001-08-22\"]");
        assertFunction("1 || ARRAY [2]", Lists.newArrayList(1L, 2L));
        assertFunction("ARRAY [2] || 1", Lists.newArrayList(2L, 1L));
        assertFunction("TRUE || ARRAY [FALSE]", Lists.newArrayList(true, false));
        assertFunction("ARRAY [FALSE] || TRUE", Lists.newArrayList(false, true));
        assertFunction("1.0 || ARRAY [2.0]", Lists.newArrayList(1.0, 2.0));
        assertFunction("ARRAY [2.0] || 1.0", Lists.newArrayList(2.0, 1.0));
        assertFunction("'puppies' || ARRAY ['kittens']", Lists.newArrayList("puppies", "kittens"));
        assertFunction("ARRAY ['kittens'] || 'puppies'", Lists.newArrayList("kittens", "puppies"));
        assertFunction("ARRAY [from_unixtime(1)] || from_unixtime(100)", ImmutableList.of(
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("from_unixtime(100) || ARRAY [from_unixtime(1)]", ImmutableList.of(
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey()),
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey())));
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
        String outOfBounds = "Index out of bounds";
        assertInvalidFunction("ARRAY [][1]", outOfBounds);
        assertInvalidFunction("ARRAY [null][-1]", outOfBounds);
        assertInvalidFunction("ARRAY [1, 2, 3][0]", outOfBounds);
        assertInvalidFunction("ARRAY [1, 2, 3][-1]", outOfBounds);
        assertInvalidFunction("ARRAY [1, 2, 3][4]", outOfBounds);

        try {
            assertFunction("ARRAY [1, 2, 3][1.1]", null);
            fail("Access to array with double subscript should fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getCode() == SemanticErrorCode.TYPE_MISMATCH);
        }

        assertFunction("ARRAY[NULL][1]", null);
        assertFunction("ARRAY[NULL, NULL, NULL][3]", null);
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
        assertFunction("ARRAY [infinity()][1]", POSITIVE_INFINITY);
        assertFunction("ARRAY [-infinity()][1]", NEGATIVE_INFINITY);
        assertFunction("ARRAY [sqrt(-1)][1]", NaN);
    }

    @Test
    public void testSort()
            throws Exception
    {
        assertFunction("ARRAY_SORT(ARRAY[2, 3, 4, 1])", ImmutableList.of(1L, 2L, 3L, 4L));
        assertFunction("ARRAY_SORT(ARRAY['z', 'f', 's', 'd', 'g'])", ImmutableList.of("d", "f", "g", "s", "z"));
        assertFunction("ARRAY_SORT(ARRAY[TRUE, FALSE])", ImmutableList.of(false, true));
        assertFunction("ARRAY_SORT(ARRAY[22.1, 11.1, 1.1, 44.1])", ImmutableList.of(1.1, 11.1, 22.1, 44.1));
        assertFunction("ARRAY_SORT(ARRAY [from_unixtime(100), from_unixtime(1), from_unixtime(200)])",
                ImmutableList.of(new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()), new SqlTimestamp(100 * 1000, TEST_SESSION.getTimeZoneKey()), new SqlTimestamp(200 * 1000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("ARRAY_SORT(ARRAY [ARRAY [1], ARRAY [2]])", ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(2L)));

        try {
            assertFunction("ARRAY_SORT(ARRAY[color('red'), color('blue')])", null);
            fail("ARRAY_SORT is not supported for arrays with incomparable elements");
        }
        catch (RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testComparison()
            throws Exception
    {
        assertFunction("ARRAY [1, 2, 3] = ARRAY [1, 2, 3]", true);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [TRUE, FALSE]", true);
        assertFunction("ARRAY [1.1, 2.2, 3.3, 4.4] = ARRAY [1.1, 2.2, 3.3, 4.4]", true);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['puppies', 'kittens']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);
        assertFunction("ARRAY [timestamp '2012-10-31 08:00 UTC'] = ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']", true);

        assertFunction("ARRAY [1, 2, 3] = ARRAY [3, 2, 1]", false);
        assertFunction("ARRAY [TRUE, FALSE] = ARRAY [FALSE, FALSE]", false);
        assertFunction("ARRAY [1.1, 2.2, 3.3] = ARRAY [11.1, 22.1, 1.1, 44.1]", false);
        assertFunction("ARRAY ['puppies', 'kittens'] = ARRAY ['z', 'f', 's', 'd', 'g']", false);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", false);

        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 20, 40, 50]", true);
        assertFunction("ARRAY [10, 20, 30] < ARRAY [10, 40]", true);
        assertFunction("ARRAY [10, 20] < ARRAY [10, 20, 30]", true);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, TRUE, TRUE]", true);
        assertFunction("ARRAY [TRUE, FALSE, FALSE] < ARRAY [TRUE, TRUE]", true);
        assertFunction("ARRAY [TRUE, FALSE] < ARRAY [TRUE, FALSE, FALSE]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] < ARRAY[1.1, 2.2, 4.4, 4.4]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] < ARRAY[1.1, 2.2, 5.5]", true);
        assertFunction("ARRAY[1.1, 2.2] < ARRAY[1.1, 2.2, 5.5]", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards', 'lizards']", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards']", true);
        assertFunction("ARRAY['puppies', 'kittens'] < ARRAY ['puppies', 'kittens', 'lizards']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);

        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20, 20]", true);
        assertFunction("ARRAY [10, 20, 30] > ARRAY [10, 20]", true);
        assertFunction("ARRAY [TRUE, TRUE, TRUE] > ARRAY [TRUE, TRUE, FALSE]", true);
        assertFunction("ARRAY [TRUE, TRUE, FALSE] > ARRAY [TRUE, TRUE]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] > ARRAY[1.1, 2.2, 2.2, 4.4]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] > ARRAY[1.1, 2.2, 3.3]", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens', 'kittens']", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']", true);

        assertFunction("ARRAY [10, 20, 30] <= ARRAY [50]", true);
        assertFunction("ARRAY [10, 20, 30] <= ARRAY [10, 20, 30]", true);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE, true]", true);
        assertFunction("ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] <= ARRAY[2.2, 5.5]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] <= ARRAY[1.1, 2.2, 3.3, 4.4]", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'lizards']", true);
        assertFunction("ARRAY['puppies', 'kittens'] <= ARRAY['puppies', 'kittens']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);

        assertFunction("ARRAY [10, 20, 30] >= ARRAY [10, 20, 30]", true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE, FALSE, TRUE]", true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] >= ARRAY[1.1, 2.2]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] >= ARRAY[1.1, 2.2, 3.3, 4.4]", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'kittens', 'kittens']", true);
        assertFunction("ARRAY['puppies', 'kittens'] >= ARRAY['puppies', 'kittens']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);

        assertFunction("ARRAY [10, 20, 30] != ARRAY [5]", true);
        assertFunction("ARRAY [TRUE, FALSE, TRUE] != ARRAY [TRUE]", true);
        assertFunction("ARRAY[1.1, 2.2, 3.3, 4.4] != ARRAY[1.1, 2.2]", true);
        assertFunction("ARRAY['puppies', 'kittens', 'lizards'] != ARRAY ['puppies', 'kittens']", true);
        assertFunction("ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']", true);

        try {
            assertFunction("ARRAY[1, NULL] = ARRAY[1, 2]", false);
            fail("ARRAY comparison not implemented for NULL values");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getCode(), StandardErrorCode.NOT_SUPPORTED.toErrorCode().getCode());
        }
   }
}
