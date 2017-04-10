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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.MapUnionAggregation.NAME;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static com.google.common.base.Preconditions.checkArgument;

public class TestMapUnionAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testSimpleWithDuplicates()
            throws Exception
    {
        MapType mapType = new MapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 43.0, "ccc", 53.0, "ddd", 13.0, "eee"),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, VARCHAR, ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 53.0, "ddd")),
                        mapBlockOf(DOUBLE, VARCHAR, ImmutableMap.of(43.0, "ccc", 53.0, "ddd", 13.0, "eee"))));

        mapType = new MapType(DOUBLE, BIGINT);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, 99L, 2.0, 99L, 3.0, 99L, 4.0, 44L),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, BIGINT, ImmutableMap.of(1.0, 99L, 2.0, 99L, 3.0, 99L)),
                        mapBlockOf(DOUBLE, BIGINT, ImmutableMap.of(1.0, 44L, 2.0, 44L, 4.0, 44L))));

        mapType = new MapType(BOOLEAN, BIGINT);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(false, 12L, true, 13L),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(BOOLEAN, BIGINT, ImmutableMap.of(false, 12L)),
                        mapBlockOf(BOOLEAN, BIGINT, ImmutableMap.of(true, 13L, false, 33L))));
    }

    @Test
    public void testSimpleWithNulls()
            throws Exception
    {
        MapType mapType = new MapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));

        Map<Object, Object> expected = mapOf(23.0, "aaa", 33.0, null, 43.0, "ccc", 53.0, "ddd", null, "eee");

        assertAggregation(
                aggFunc,
                expected,
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, VARCHAR, mapOf(23.0, "aaa", 33.0, null, 53.0, "ddd")),
                        null,
                        mapBlockOf(DOUBLE, VARCHAR, mapOf(43.0, "ccc", 53.0, "ddd", null, "eee"))));
    }

    @Test
    public void testStructural()
            throws Exception
    {
        MapType mapType = new MapType(DOUBLE, new ArrayType(VARCHAR));
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(
                        1.0, ImmutableList.of("a", "b"),
                        2.0, ImmutableList.of("c", "d"),
                        3.0, ImmutableList.of("e", "f"),
                        4.0, ImmutableList.of("r", "s")),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(
                                DOUBLE,
                                new ArrayType(VARCHAR),
                                ImmutableMap.of(
                                        1.0,
                                        ImmutableList.of("a", "b"),
                                        2.0,
                                        ImmutableList.of("c", "d"),
                                        3.0,
                                        ImmutableList.of("e", "f"))),
                        mapBlockOf(
                                DOUBLE,
                                new ArrayType(VARCHAR),
                                ImmutableMap.of(
                                        1.0,
                                        ImmutableList.of("x", "y"),
                                        4.0,
                                        ImmutableList.of("r", "s"),
                                        3.0,
                                        ImmutableList.of("w", "z")))));

        mapType = new MapType(DOUBLE, new MapType(VARCHAR, VARCHAR));
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(
                        1.0, ImmutableMap.of("a", "b"),
                        2.0, ImmutableMap.of("c", "d"),
                        3.0, ImmutableMap.of("e", "f")),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(
                                DOUBLE,
                                new MapType(VARCHAR, VARCHAR),
                                ImmutableMap.of(
                                        1.0,
                                        ImmutableMap.of("a", "b"),
                                        2.0,
                                        ImmutableMap.of("c", "d"))),
                        mapBlockOf(
                                DOUBLE,
                                new MapType(VARCHAR, VARCHAR),
                                ImmutableMap.of(
                                        3.0,
                                        ImmutableMap.of("e", "f")))));

        mapType = new MapType(new ArrayType(VARCHAR), DOUBLE);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), mapType.getTypeSignature()));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(
                        ImmutableList.of("a", "b"), 1.0,
                        ImmutableList.of("c", "d"), 2.0,
                        ImmutableList.of("e", "f"), 3.0),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(
                                new ArrayType(VARCHAR),
                                DOUBLE,
                                ImmutableMap.of(
                                        ImmutableList.of("a", "b"),
                                        1.0,
                                        ImmutableList.of("e", "f"),
                                        3.0)),
                        mapBlockOf(
                                new ArrayType(VARCHAR),
                                DOUBLE,
                                ImmutableMap.of(
                                        ImmutableList.of("c", "d"),
                                        2.0))));
    }

    private static Map<Object, Object> mapOf(Object... entries)
    {
        checkArgument(entries.length % 2 == 0);
        HashMap<Object, Object> map = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(entries[i], entries[i + 1]);
        }
        return map;
    }
}
