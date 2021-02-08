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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> allTypes = metadata.getFunctionAndTypeManager().getTypes().stream().collect(toImmutableSet());

        for (Type valueType : allTypes) {
            assertNotNull(getAggregation(valueType));
        }
    }

    @Test
    public void testNullBoolean()
    {
        InternalAggregationFunction booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(
                booleanAgg,
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
    {
        InternalAggregationFunction booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(
                booleanAgg,
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
    {
        InternalAggregationFunction longAgg = getAggregation(BIGINT);
        assertAggregation(
                longAgg,
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
    {
        InternalAggregationFunction longAgg = getAggregation(BIGINT);
        assertAggregation(
                longAgg,
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
    {
        InternalAggregationFunction doubleAgg = getAggregation(DOUBLE);
        assertAggregation(
                doubleAgg,
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
    {
        InternalAggregationFunction doubleAgg = getAggregation(DOUBLE);
        assertAggregation(
                doubleAgg,
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
    {
        InternalAggregationFunction stringAgg = getAggregation(VARCHAR);
        assertAggregation(
                stringAgg,
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
    {
        InternalAggregationFunction stringAgg = getAggregation(VARCHAR);
        assertAggregation(
                stringAgg,
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
    {
        InternalAggregationFunction arrayAgg = getAggregation(new ArrayType(BIGINT));
        assertAggregation(
                arrayAgg,
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
    {
        InternalAggregationFunction arrayAgg = getAggregation(new ArrayType(BIGINT));
        assertAggregation(
                arrayAgg,
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
    {
        InternalAggregationFunction intAgg = getAggregation(INTEGER);
        assertAggregation(
                intAgg,
                3,
                createIntsBlock(3, 3, null));
    }

    private static InternalAggregationFunction getAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("arbitrary", fromTypes(arguments)));
    }
}
