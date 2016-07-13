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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static java.lang.Double.doubleToLongBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestMapOperators
        extends AbstractTestFunctions
{
    private TestMapOperators()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @SqlType(StandardTypes.JSON)
    public static Slice uncheckedToJson(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        return slice;
    }

    @Test
    public void testStackRepresentation()
            throws Exception
    {
        Block array = arrayBlockOf(BIGINT, 1L, 2L);
        Block actualBlock = mapBlockOf(DOUBLE, new ArrayType(BIGINT), ImmutableMap.of(1.0, array));
        DynamicSliceOutput actualSliceOutput = new DynamicSliceOutput(100);
        writeBlock(actualSliceOutput, actualBlock);

        Block expectedBlock = new InterleavedBlockBuilder(ImmutableList.<Type>of(DOUBLE, new ArrayType(BIGINT)), new BlockBuilderStatus(), 3)
                .writeLong(doubleToLongBits(1.0))
                .closeEntry()
                .writeObject(
                        BIGINT
                        .createBlockBuilder(new BlockBuilderStatus(), 1)
                        .writeLong(1L)
                        .closeEntry()
                        .writeLong(2L)
                        .closeEntry()
                        .build()
                )
                .closeEntry()
                .build();
        DynamicSliceOutput expectedSliceOutput = new DynamicSliceOutput(100);
        writeBlock(expectedSliceOutput, expectedBlock);

        assertEquals(actualSliceOutput.slice(), expectedSliceOutput.slice());
    }

    @Test
    public void testConstructor()
            throws Exception
    {
        assertFunction("MAP(ARRAY ['1','3'], ARRAY [2,4])", new MapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 2, "3", 4));
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 2);
        map.put(3, null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])", new MapType(INTEGER, INTEGER), map);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0, 4.0])", new MapType(INTEGER, DOUBLE), ImmutableMap.of(1, 2.0, 3, 4.0));
        assertFunction("MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]])",
                new MapType(DOUBLE, new ArrayType(INTEGER)),
                ImmutableMap.of(1.0, ImmutableList.of(1, 2), 2.0, ImmutableList.of(3)));
        assertFunction("MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[BIGINT '1', BIGINT '2'], ARRAY[ BIGINT '3' ]])",
                new MapType(DOUBLE, new ArrayType(BIGINT)),
                ImmutableMap.of(1.0, ImmutableList.of(1L, 2L), 2.0, ImmutableList.of(3L)));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])", new MapType(createVarcharType(7), createVarcharType(7)), ImmutableMap.of("puppies", "kittens"));
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY[2,4])", new MapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 2, false, 4));
        assertFunction("MAP(ARRAY['1', '100'], ARRAY[from_unixtime(1), from_unixtime(100)])", new MapType(createVarcharType(3), TIMESTAMP), ImmutableMap.of(
                "1",
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                "100",
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0, 100.0])", new MapType(TIMESTAMP, DOUBLE), ImmutableMap.of(
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
        assertFunction("CARDINALITY(MAP(ARRAY ['1','3'], ARRAY [2,4]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY[2, NULL]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY [1, 3], ARRAY [2.0, 4.0]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]]))", BIGINT, 2L);
        assertFunction("CARDINALITY(MAP(ARRAY['puppies'], ARRAY['kittens']))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY[TRUE], ARRAY[2]))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]))", BIGINT, 1L);
        assertFunction("CARDINALITY(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]))", BIGINT, 1L);
    }

    @Test
    public void testMapToJson()
            throws Exception
    {
        assertFunction("CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)", JSON, "{\"1\":2,\"3\":4,\"5\":6,\"7\":8}");
        assertFunction("CAST(MAP(ARRAY [1, 3], ARRAY[2, NULL]) AS JSON)", JSON, "{\"1\":2,\"3\":null}");
        assertFunction("CAST(MAP(ARRAY [1, 3], ARRAY [2.0, 4.0]) AS JSON)", JSON, "{\"1\":2.0,\"3\":4.0}");
        assertFunction("CAST(MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]]) AS JSON)", JSON, "{\"1.0\":[1,2],\"2.0\":[3]}");
        assertFunction("CAST(MAP(ARRAY['puppies'], ARRAY['kittens']) AS JSON)", JSON, "{\"puppies\":\"kittens\"}");
        assertFunction("CAST(MAP(ARRAY[TRUE], ARRAY[2]) AS JSON)", JSON, "{\"true\":2}");
        assertFunction("CAST(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]) AS JSON)", JSON, "{\"1\":\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()) + "\"}");
        assertFunction("CAST(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]) AS JSON)", JSON, "{\"" + new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()) + "\":1.0}");
    }

    @Test
    public void testJsonToMap()
            throws Exception
    {
        assertFunction("CAST(JSON '{\"1\":2, \"3\": 4}' AS MAP<BIGINT, BIGINT>)",
                new MapType(BIGINT, BIGINT),
                ImmutableMap.of(1L, 2L, 3L, 4L));
        assertFunction("CAST(JSON '{\"1\":2.0, \"3\": 4.0}' AS MAP<BIGINT, DOUBLE>)",
                new MapType(BIGINT, DOUBLE),
                ImmutableMap.of(1L, 2.0, 3L, 4.0));
        assertFunction("CAST(JSON '{\"1\":[2, 3], \"4\": [5]}' AS MAP<BIGINT, ARRAY<BIGINT>>)",
                new MapType(BIGINT, new ArrayType(BIGINT)),
                ImmutableMap.of(1L, ImmutableList.of(2L, 3L), 4L, ImmutableList.of(5L)));
        assertFunction("CAST(JSON '{\"puppies\":\"kittens\"}' AS MAP<VARCHAR, VARCHAR>)",
                new MapType(VARCHAR, VARCHAR),
                ImmutableMap.of("puppies", "kittens"));
        assertFunction("CAST(JSON '{\"true\":\"kittens\"}' AS MAP<BOOLEAN, VARCHAR>)",
                new MapType(BOOLEAN, VARCHAR),
                ImmutableMap.of(true, "kittens"));
        assertFunction("CAST(JSON 'null' AS MAP<BOOLEAN, VARCHAR>)",
                new MapType(BOOLEAN, VARCHAR),
                null);
        assertFunction("CAST(JSON '{\"k1\": 5, \"k2\":[1, 2, 3], \"k3\":\"e\", \"k4\":{\"a\": \"b\"}, \"k5\":null, \"k6\":\"null\", \"k7\":[null]}' AS MAP<VARCHAR, JSON>)",
                new MapType(VARCHAR, JSON),
                ImmutableMap.builder()
                        .put("k1", "5")
                        .put("k2", "[1,2,3]")
                        .put("k3", "\"e\"")
                        .put("k4", "{\"a\":\"b\"}")
                        .put("k5", "null")
                        .put("k6", "\"null\"")
                        .put("k7", "[null]")
                        .build()
        );

        // These two tests verifies that partial json cast preserves input order
        // The second test should never happen in real life because valid json in presto requires natural key ordering.
        // However, it is added to make sure that the order in the first test is not a coincidence.
        assertFunction("CAST(JSON '{\"k1\": {\"1klmnopq\":1, \"2klmnopq\":2, \"3klmnopq\":3, \"4klmnopq\":4, \"5klmnopq\":5, \"6klmnopq\":6, \"7klmnopq\":7}}' AS MAP<VARCHAR, JSON>)",
                new MapType(VARCHAR, JSON),
                ImmutableMap.of("k1", "{\"1klmnopq\":1,\"2klmnopq\":2,\"3klmnopq\":3,\"4klmnopq\":4,\"5klmnopq\":5,\"6klmnopq\":6,\"7klmnopq\":7}")
        );
        assertFunction("CAST(unchecked_to_json('{\"k1\": {\"7klmnopq\":7, \"6klmnopq\":6, \"5klmnopq\":5, \"4klmnopq\":4, \"3klmnopq\":3, \"2klmnopq\":2, \"1klmnopq\":1}}') AS MAP<VARCHAR, JSON>)",
                new MapType(VARCHAR, JSON),
                ImmutableMap.of("k1", "{\"7klmnopq\":7,\"6klmnopq\":6,\"5klmnopq\":5,\"4klmnopq\":4,\"3klmnopq\":3,\"2klmnopq\":2,\"1klmnopq\":1}")
        );

        assertInvalidCast("CAST(JSON '{\"true\":\"kittens\"}' AS MAP<BOOLEAN, VARBINARY>)");
        assertInvalidCast("CAST(JSON '{\"[1, 2]\": 1}' AS MAP<ARRAY<BIGINT>, BIGINT>)");
    }

    @Test
    public void testElementAt()
            throws Exception
    {
        assertFunction("element_at(MAP(CAST(ARRAY [] AS ARRAY(BIGINT)), CAST(ARRAY [] AS ARRAY(BIGINT))), 1)", BIGINT, null);
        assertFunction("element_at(MAP(ARRAY [1], ARRAY [1]), 2)", INTEGER, null);
        assertFunction("element_at(MAP(ARRAY [1], ARRAY [null]), 1)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [1.0], ARRAY [null]), 1.0)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [TRUE], ARRAY [null]), TRUE)", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY['puppies'], ARRAY [null]), 'puppies')", UNKNOWN, null);
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY [2, 4]), 3)", INTEGER, 4);
        assertFunction("element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4]), 3)", BIGINT, 4L);
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY[2, NULL]), 3)", INTEGER, null);
        assertFunction("element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY[2, NULL]), 3)", INTEGER, null);
        assertFunction("element_at(MAP(ARRAY [1, 3], ARRAY [2.0, 4.0]), 1)", DOUBLE, 2.0);
        assertFunction("element_at(MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]]), 1.0)", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("element_at(MAP(ARRAY['puppies'], ARRAY['kittens']), 'puppies')", createVarcharType(7), "kittens");
        assertFunction("element_at(MAP(ARRAY[TRUE,FALSE],ARRAY[2,4]), TRUE)", INTEGER, 2);
        assertFunction("element_at(MAP(ARRAY['1', '100'], ARRAY[from_unixtime(1), from_unixtime(100)]), '1')", TIMESTAMP, new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()));
        assertFunction("element_at(MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0, 100.0]), from_unixtime(1))", DOUBLE, 1.0);
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        assertFunction("MAP(ARRAY [1], ARRAY [null])[1]", UNKNOWN, null);
        assertFunction("MAP(ARRAY [1.0], ARRAY [null])[1.0]", UNKNOWN, null);
        assertFunction("MAP(ARRAY [TRUE], ARRAY [null])[TRUE]", UNKNOWN, null);
        assertFunction("MAP(ARRAY['puppies'], ARRAY [null])['puppies']", UNKNOWN, null);
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [1])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [CAST(null as bigint)], ARRAY [CAST(null as bigint)])", "map key cannot be null");
        assertInvalidFunction("MAP(ARRAY [1,null], ARRAY [null,2])", "map key cannot be null");
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2, 4])[3]", INTEGER, 4);
        assertFunction("MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4])[3]", BIGINT, 4L);
        assertFunction("MAP(ARRAY [1, 3], ARRAY[2, NULL])[3]", INTEGER, null);
        assertFunction("MAP(ARRAY [BIGINT '1', 3], ARRAY[2, NULL])[3]", INTEGER, null);
        assertFunction("MAP(ARRAY [1, 3], ARRAY [2.0, 4.0])[1]", DOUBLE, 2.0);
        assertFunction("MAP(ARRAY[1.0, 2.0], ARRAY[ ARRAY[1, 2], ARRAY[3]])[1.0]", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("MAP(ARRAY['puppies'], ARRAY['kittens'])['puppies']", createVarcharType(7), "kittens");
        assertFunction("MAP(ARRAY[TRUE,FALSE],ARRAY[2,4])[TRUE]", INTEGER, 2);
        assertFunction("MAP(ARRAY['1', '100'], ARRAY[from_unixtime(1), from_unixtime(100)])['1']", TIMESTAMP, new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()));
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0, 100.0])[from_unixtime(1)]", DOUBLE, 1.0);
    }

    @Test
    public void testMapKeys()
            throws Exception
    {
        assertFunction("MAP_KEYS(MAP(ARRAY['1', '3'], ARRAY['2', '4']))",  new ArrayType(createVarcharType(1)), ImmutableList.of("1", "3"));
        assertFunction("MAP_KEYS(MAP(ARRAY[1.0, 2.0], ARRAY[ARRAY[1, 2], ARRAY[3]]))", new ArrayType(DOUBLE), ImmutableList.of(1.0, 2.0));
        assertFunction("MAP_KEYS(MAP(ARRAY['puppies'], ARRAY['kittens']))", new ArrayType(createVarcharType(7)), ImmutableList.of("puppies"));
        assertFunction("MAP_KEYS(MAP(ARRAY[TRUE], ARRAY[2]))", new ArrayType(BOOLEAN), ImmutableList.of(true));
        assertFunction("MAP_KEYS(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0]))", new ArrayType(TIMESTAMP), ImmutableList.of(new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("MAP_KEYS(MAP(ARRAY[CAST('puppies' as varbinary)], ARRAY['kittens']))", new ArrayType(VARBINARY), ImmutableList.of(new SqlVarbinary("puppies".getBytes(UTF_8))));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,2],  ARRAY[ARRAY[1, 2], ARRAY[3]]))", new ArrayType(INTEGER), ImmutableList.of(1, 2));
        assertFunction("MAP_KEYS(MAP(ARRAY[1,4], ARRAY[MAP(ARRAY[2], ARRAY[3]), MAP(ARRAY[5], ARRAY[6])]))",  new ArrayType(INTEGER), ImmutableList.of(1, 4));
        assertFunction("MAP_KEYS(MAP(ARRAY [ARRAY [1], ARRAY [2, 3]],  ARRAY [ARRAY [3, 4], ARRAY [5]]))", new ArrayType(new ArrayType(INTEGER)), ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2, 3)));
    }

    @Test
    public void testMapValues()
            throws Exception
    {
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[TRUE, FALSE, NULL]]))",
                new ArrayType(new ArrayType(BOOLEAN)),
                ImmutableList.of(Lists.newArrayList(true, false, null)));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[ARRAY[1, 2]]]))",
                new ArrayType(new ArrayType(new ArrayType(INTEGER))),
                ImmutableList.of(ImmutableList.of(ImmutableList.of(1, 2))));
        assertFunction("MAP_VALUES(MAP(ARRAY [1, 3], ARRAY ['2', '4']))",
                new ArrayType(createVarcharType(1)),
                ImmutableList.of("2", "4"));
        assertFunction("MAP_VALUES(MAP(ARRAY[1.0,2.0], ARRAY[ARRAY[1, 2], ARRAY[3]]))",
                new ArrayType(new ArrayType(INTEGER)),
                ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3)));
        assertFunction("MAP_VALUES(MAP(ARRAY['puppies'], ARRAY['kittens']))",
                new ArrayType(createVarcharType(7)),
                ImmutableList.of("kittens"));
        assertFunction("MAP_VALUES(MAP(ARRAY[TRUE], ARRAY[2]))",
                new ArrayType(INTEGER),
                ImmutableList.of(2));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[NULL]))",
                new ArrayType(UNKNOWN),
                Lists.newArrayList((Object) null));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[TRUE]))",
                new ArrayType(BOOLEAN),
                ImmutableList.of(true));
        assertFunction("MAP_VALUES(MAP(ARRAY['1'], ARRAY[1.0]))",
                new ArrayType(DOUBLE),
                ImmutableList.of(1.0));
        assertFunction("MAP_VALUES(MAP(ARRAY['1', '2'], ARRAY[ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]]))",
                new ArrayType(new ArrayType(DOUBLE)),
                ImmutableList.of(ImmutableList.of(1.0, 2.0), ImmutableList.of(3.0, 4.0)));
    }

    @Test
    public void testEquals()
            throws Exception
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) = MAP(ARRAY[2], ARRAY[1])", BOOLEAN, false);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1, 3], ARRAY[2, 4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[1], ARRAY[2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[4, 2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[2, 4])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) = MAP(ARRAY['3', '1'], ARRAY[4.0, 2.0])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) = MAP(ARRAY['3', '1'], ARRAY[2.0, 4.0])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0, 1.0], ARRAY[FALSE, TRUE])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0, 1.0], ARRAY[TRUE, FALSE])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(100), from_unixtime(1)])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(1), from_unixtime(100)])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['cat', 'dog']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, false);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", BOOLEAN, null);
    }

    @Test
    public void testNotEquals()
            throws Exception
    {
        // single item
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[3], ARRAY[1]) != MAP(ARRAY[2], ARRAY[1])", BOOLEAN, true);

        // multiple items
        assertFunction("MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1, 3], ARRAY[2, 4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[1], ARRAY[2])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[4, 2])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[2, 4])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) != MAP(ARRAY['3', '1'], ARRAY[4.0, 2.0])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['1', '3'], ARRAY[2.0, 4.0]) != MAP(ARRAY['3', '1'], ARRAY[2.0, 4.0])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0, 1.0], ARRAY[FALSE, TRUE])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0, 1.0], ARRAY[TRUE, FALSE])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(100), from_unixtime(1)])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[1.0, 3.0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0, 1.0], ARRAY[from_unixtime(1), from_unixtime(100)])", BOOLEAN, true);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])", BOOLEAN, false);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])", BOOLEAN, true);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])", BOOLEAN, false);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])", BOOLEAN, true);

        // nulls
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])", BOOLEAN, null);
        assertFunction("MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])", BOOLEAN, null);
        assertFunction("MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])", BOOLEAN, null);
    }

    @Test
    public void testMapConcat()
            throws Exception
    {
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (CAST(ARRAY [] AS ARRAY(BOOLEAN)), CAST(ARRAY [] AS ARRAY(INTEGER))))", new MapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 1));
        // <BOOLEAN, INTEGER> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))", new MapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 20));
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))", new MapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 20));
        assertFunction("MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE], ARRAY [10]))", new MapType(BOOLEAN, INTEGER), ImmutableMap.of(true, 10, false, 2));

        // <VARCHAR, INTEGER> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3'], ARRAY [1, 2, 3]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))", new MapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))", new MapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 40));
        assertFunction("MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3'], ARRAY [10, 20, 30]))", new MapType(createVarcharType(1), INTEGER), ImmutableMap.of("1", 10, "2", 20, "3", 30, "4", 4));

        // <INTEGER, ARRAY<DOUBLE>> Tests
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0], ARRAY [20.0], ARRAY [30.0], ARRAY [40.0]]))", new MapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0], ARRAY [4.0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0], ARRAY [20.0], ARRAY [30.0], ARRAY [40.0]]))", new MapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(40.0)));
        assertFunction("MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0], ARRAY [4.0]]), MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [10.0], ARRAY [20.0], ARRAY [30.0]]))", new MapType(INTEGER, new ArrayType(DOUBLE)), ImmutableMap.of(1, ImmutableList.of(10.0), 2, ImmutableList.of(20.0), 3, ImmutableList.of(30.0), 4, ImmutableList.of(4.0)));

        // <ARRAY<DOUBLE>, VARCHAR> Tests
        assertFunction(
                "MAP_CONCAT(MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0], ARRAY [4.0]], ARRAY ['10', '20', '30', '40']))",
                new MapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));
        assertFunction(
                "MAP_CONCAT(MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0], ARRAY [4.0]], ARRAY ['10', '20', '30', '40']))",
                new MapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "40"));
        assertFunction("MAP_CONCAT(MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0], ARRAY [4.0]], ARRAY ['1', '2', '3', '4']), MAP (ARRAY [ARRAY [1.0], ARRAY [2.0], ARRAY [3.0]], ARRAY ['10', '20', '30']))",
                new MapType(new ArrayType(DOUBLE), createVarcharType(2)),
                ImmutableMap.of(ImmutableList.of(1.0), "10", ImmutableList.of(2.0), "20", ImmutableList.of(3.0), "30", ImmutableList.of(4.0), "4"));
    }

    @Test
    public void testMapToMapCast()
    {
        assertFunction("CAST(MAP(ARRAY['1', '100'], ARRAY[true, false]) AS MAP<varchar,bigint>)", new MapType(VARCHAR, BIGINT), ImmutableMap.of("1", 1L, "100", 0L));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[1,2]) AS MAP<bigint, boolean>)", new MapType(BIGINT, BOOLEAN), ImmutableMap.of(1L, true, 2L, true));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[array[1],array[2]]) AS MAP<bigint, array<boolean>>)", new MapType(BIGINT, new ArrayType(BOOLEAN)), ImmutableMap.of(1L, ImmutableList.of(true), 2L, ImmutableList.of(true)));
        assertFunction("CAST(MAP(ARRAY[1], ARRAY[MAP(ARRAY[1.0], ARRAY[false])]) AS MAP<varchar, MAP(bigint,bigint)>)", new MapType(VARCHAR, new MapType(BIGINT, BIGINT)), ImmutableMap.of("1", ImmutableMap.of(1L, 0L)));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[DATE '2016-01-02', DATE '2016-02-03']) AS MAP(bigint, varchar))", new MapType(BIGINT, VARCHAR), ImmutableMap.of(1L, "2016-01-02", 2L, "2016-02-03"));
        assertFunction("CAST(MAP(ARRAY[1,2], ARRAY[TIMESTAMP '2016-01-02 01:02:03', TIMESTAMP '2016-02-03 03:04:05']) AS MAP(bigint, varchar))", new MapType(BIGINT, VARCHAR), ImmutableMap.of(1L, "2016-01-02 01:02:03.000", 2L, "2016-02-03 03:04:05.000"));
        assertInvalidCast("CAST(MAP(ARRAY[1, 2], ARRAY[6, 9]) AS MAP<boolean, bigint>)", "duplicate keys");
    }
}
