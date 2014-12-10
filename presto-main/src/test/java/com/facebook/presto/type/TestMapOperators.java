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
import com.facebook.presto.spi.type.SqlVarbinary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.MapType.rawValueSlicesToStackRepresentation;
import static org.testng.Assert.assertEquals;

public class TestMapOperators
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
        Slice array = ArrayType.toStackRepresentation(ImmutableList.of(1L, 2L));
        Slice slice = rawValueSlicesToStackRepresentation(ImmutableMap.of(1.0, array));
        assertEquals(slice, Slices.utf8Slice("{\"1.0\":[1,2]}"));
    }

    @Test
    public void testConstructor()
            throws Exception
    {
        assertFunction("MAP(ARRAY ['1','3'], ARRAY [2,4])", ImmutableMap.of("1", 2L, "3", 4L));
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, 2L);
        map.put(3L, null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])", map);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0, 4.0])", ImmutableMap.of(1L, 2.0, 3L, 4.0));
        assertFunction("MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]])", ImmutableMap.of(1.0, ImmutableList.of(1L, 2L), 2.0, ImmutableList.of(3L)));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])", ImmutableMap.of("puppies", "kittens"));
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY[2,4])", ImmutableMap.of(true, 2L, false, 4L));
        assertFunction("MAP(ARRAY['1', '100'], ARRAY[from_unixtime(1), from_unixtime(100)])", ImmutableMap.of(
                "1",
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                "100",
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0, 100.0])", ImmutableMap.of(
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                1.0,
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey()),
                100.0));

        assertInvalidFunction("MAP(ARRAY [1], ARRAY [2, 4])", "Key and value arrays must be the same length");
    }

    @Test
    public void testCardinality()
            throws Exception
    {
        assertFunction("CARDINALITY(MAP(ARRAY ['1','3'], ARRAY [2,4]))", 2);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY[2, NULL]))", 2);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY [2.0, 4.0]))", 2);
        assertFunction("CARDINALITY(MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]]))", 2);
        assertFunction("CARDINALITY(MAP(ARRAY['puppies'], ARRAY['kittens']))", 1);
        assertFunction("CARDINALITY(MAP(ARRAY[TRUE], ARRAY[2]))", 1);
        assertFunction("CARDINALITY(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]))", 1);
        assertFunction("CARDINALITY(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]))", 1);
    }

    @Test
    public void testMapToJson()
            throws Exception
    {
        assertFunction("CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)", "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)", "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY [1, 3], ARRAY[2, NULL]) AS JSON)", "{\"1\":2,\"3\":null}");
        assertFunction("CAST(MAP(ARRAY [1, 3], ARRAY [2.0, 4.0]) AS JSON)", "{\"1\":2.0,\"3\":4.0}");
        assertFunction("CAST(MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]]) AS JSON)", "{\"1.0\":[1,2],\"2.0\":[3]}");
        assertFunction("CAST(MAP(ARRAY['puppies'], ARRAY['kittens']) AS JSON)", "{\"puppies\":\"kittens\"}");
        assertFunction("CAST(MAP(ARRAY[TRUE], ARRAY[2]) AS JSON)", "{\"true\":2}");
        assertFunction("CAST(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]) AS JSON)", "{\"1\":\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()).toString() + "\"}");
        assertFunction("CAST(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]) AS JSON)", "{\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()).toString() + "\":1.0}");
    }

    @Test
    public void testJsonToMap()
            throws Exception
    {
        assertFunction("CAST(CAST('{\"1\":2, \"3\": 4}' AS JSON) AS MAP<BIGINT, BIGINT>)", ImmutableMap.of(1L, 2L, 3L, 4L));
        assertFunction("CAST(CAST('{\"1\":2.0, \"3\": 4.0}' AS JSON) AS MAP<BIGINT, DOUBLE>)", ImmutableMap.of(1L, 2.0, 3L, 4.0));
        assertFunction("CAST(CAST('{\"1\":[2, 3], \"4\": [5]}' AS JSON) AS MAP<BIGINT, ARRAY<BIGINT>>)", ImmutableMap.of(1L, ImmutableList.of(2L, 3L), 4L, ImmutableList.of(5L)));
        assertFunction("CAST(CAST('{\"puppies\":\"kittens\"}' AS JSON) AS MAP<VARCHAR, VARCHAR>)", ImmutableMap.of("puppies", "kittens"));
        assertFunction("CAST(CAST('{\"true\":\"kittens\"}' AS JSON) AS MAP<BOOLEAN, VARCHAR>)", ImmutableMap.of(true, "kittens"));
        assertFunction("CAST(CAST('null' AS JSON) AS MAP<BOOLEAN, VARCHAR>)", null);
        assertInvalidFunction("CAST(CAST('{\"true\":\"kittens\"}' AS JSON) AS MAP<BOOLEAN, VARBINARY>)");
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [1])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [CAST(null as bigint)])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [1,null], ARRAY [null,2])", "map key cannot be null");
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2, 4])[3]", 4L);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])[3]", null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0, 4.0])[1]", 2.0);
        assertFunction("MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]])[1.0]", ImmutableList.of(1L, 2L));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])['puppies']", "kittens");
        assertFunction("MAP(ARRAY[TRUE,FALSE],ARRAY[2,4])[TRUE]", 2L);
        assertFunction("MAP(ARRAY['1', '100'], ARRAY[from_unixtime(1), from_unixtime(100)])['1']", new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()));
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0, 100.0])[from_unixtime(1)]", 1.0);
    }

    @Test
    public void testMapKeys()
            throws Exception
    {
        assertFunction("MAP_KEYS(MAP(ARRAY['1', '3'], ARRAY['2', '4']))",  ImmutableList.of("1", "3"));
        assertFunction("MAP_KEYS(MAP(ARRAY[1.0, 2.0], ARRAY[ARRAY[1, 2], ARRAY[3]]))", ImmutableList.of(1.0, 2.0));
        assertFunction("MAP_KEYS(MAP(ARRAY['puppies'], ARRAY['kittens']))", ImmutableList.of("puppies"));
        assertFunction("MAP_KEYS(MAP(ARRAY[TRUE], ARRAY[2]))", ImmutableList.of(true));
        assertFunction("MAP_KEYS(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]))", ImmutableList.of(new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("MAP_KEYS(MAP(ARRAY[CAST('puppies' as varbinary)], ARRAY['kittens']))", ImmutableList.of(new SqlVarbinary("puppies".getBytes("utf-8"))));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,2],  ARRAY[ARRAY[1, 2], ARRAY[3]]))", ImmutableList.of(1L, 2L));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,4], ARRAY[MAP(ARRAY[2], ARRAY[3]), MAP(ARRAY[5], ARRAY[6])]))",  ImmutableList.of(1L, 4L));
    }

    @Test
    public void testMapValues()
            throws Exception
    {
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[TRUE, FALSE, NULL]]))", ImmutableList.of(Lists.newArrayList(true, false, null)));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[ARRAY[1, 2]]]))", ImmutableList.of(ImmutableList.of(ImmutableList.of(1L, 2L))));
        assertFunction("MAP_VALUES(MAP(ARRAY [1, 3], ARRAY ['2', '4']))", ImmutableList.of("2", "4"));
        assertFunction("MAP_VALUES(MAP(ARRAY[1.0,2.0], ARRAY[ARRAY[1, 2], ARRAY[3]]))", ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L)));
        assertFunction("MAP_VALUES(MAP(ARRAY['puppies'], ARRAY['kittens']))", ImmutableList.of("kittens"));
        assertFunction("MAP_VALUES(MAP(ARRAY[TRUE], ARRAY[2]))", ImmutableList.of(2L));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[NULL]))", Lists.newArrayList((Object) null));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[TRUE]))", ImmutableList.of(true));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[1.0]))", ImmutableList.of(1.0));
        assertFunction("MAP_VALUES(MAP(ARRAY['1', '2'], ARRAY[ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]]))", ImmutableList.of(ImmutableList.of(1.0, 2.0), ImmutableList.of(3.0, 4.0)));
    }

    @Test
    public void testEquals()
            throws Exception
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[2])", true);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[4])", false);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) = MAP(ARRAY[2], ARRAY[1])", false);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1, 3], ARRAY[2, 4])", false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[1], ARRAY[2])", false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[4, 2])", true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[2, 4])", false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) = MAP(ARRAY['3', '1'], ARRAY[4.0, 2.0])", true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) = MAP(ARRAY['3', '1'], ARRAY[2.0, 4.0])", false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0, 1.0], ARRAY[FALSE, TRUE])", true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0, 1.0], ARRAY[TRUE, FALSE])", false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(100), from_unixtime(1)])", true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(1), from_unixtime(100)])", false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", false);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", null);
    }

    @Test
    public void testNotEquals()
            throws Exception
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[2])", false);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[4])", true);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) != MAP(ARRAY[2], ARRAY[1])", true);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1, 3], ARRAY[2, 4])", true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[1], ARRAY[2])", true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[4, 2])", false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[2, 4])", true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) != MAP(ARRAY['3', '1'], ARRAY[4.0, 2.0])", false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) != MAP(ARRAY['3', '1'], ARRAY[2.0, 4.0])", true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0, 1.0], ARRAY[FALSE, TRUE])", false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0, 1.0], ARRAY[TRUE, FALSE])", true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(100), from_unixtime(1)])", false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(1), from_unixtime(100)])", true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", true);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", null);
    }
}
