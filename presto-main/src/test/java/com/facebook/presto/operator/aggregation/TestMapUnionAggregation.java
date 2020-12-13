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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.MapUnionAggregation.NAME;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.base.Preconditions.checkArgument;

public class TestMapUnionAggregation
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testSimpleWithDuplicates()
    {
        MapType mapType = mapType(DOUBLE, VARCHAR);
        FunctionHandle functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        InternalAggregationFunction aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
        assertAggregation(
                aggFunc,
                ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 43.0, "ccc", 53.0, "ddd", 13.0, "eee"),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, VARCHAR, ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 53.0, "ddd")),
                        mapBlockOf(DOUBLE, VARCHAR, ImmutableMap.of(43.0, "ccc", 53.0, "ddd", 13.0, "eee"))));

        mapType = mapType(DOUBLE, BIGINT);
        functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, 99L, 2.0, 99L, 3.0, 99L, 4.0, 44L),
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, BIGINT, ImmutableMap.of(1.0, 99L, 2.0, 99L, 3.0, 99L)),
                        mapBlockOf(DOUBLE, BIGINT, ImmutableMap.of(1.0, 44L, 2.0, 44L, 4.0, 44L))));

        mapType = mapType(BOOLEAN, BIGINT);
        functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
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
    {
        MapType mapType = mapType(DOUBLE, VARCHAR);
        FunctionHandle functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        InternalAggregationFunction aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);

        Map<Object, Object> expected = mapOf(23.0, "aaa", 33.0, null, 43.0, "ccc", 53.0, "ddd");

        assertAggregation(
                aggFunc,
                expected,
                arrayBlockOf(
                        mapType,
                        mapBlockOf(DOUBLE, VARCHAR, mapOf(23.0, "aaa", 33.0, null, 53.0, "ddd")),
                        null,
                        mapBlockOf(DOUBLE, VARCHAR, mapOf(43.0, "ccc", 53.0, "ddd"))));
    }

    @Test
    public void testStructural()
    {
        MapType mapType = mapType(DOUBLE, new ArrayType(VARCHAR));
        FunctionHandle functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        InternalAggregationFunction aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
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

        mapType = mapType(DOUBLE, mapType(VARCHAR, VARCHAR));
        functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
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
                                mapType(VARCHAR, VARCHAR),
                                ImmutableMap.of(
                                        1.0,
                                        ImmutableMap.of("a", "b"),
                                        2.0,
                                        ImmutableMap.of("c", "d"))),
                        mapBlockOf(
                                DOUBLE,
                                mapType(VARCHAR, VARCHAR),
                                ImmutableMap.of(
                                        3.0,
                                        ImmutableMap.of("e", "f")))));

        mapType = mapType(new ArrayType(VARCHAR), DOUBLE);
        functionHandle = FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(mapType));
        aggFunc = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(functionHandle);
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
