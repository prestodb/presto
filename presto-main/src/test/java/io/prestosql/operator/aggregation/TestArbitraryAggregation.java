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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.block.BlockAssertions.createArrayBigintBlock;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createIntsBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> allTypes = metadata.getTypeManager().getTypes().stream().collect(toImmutableSet());

        for (Type valueType : allTypes) {
            assertNotNull(metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("arbitrary", AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature())));
        }
    }

    @Test
    public void testNullBoolean()
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
    {
        InternalAggregationFunction longAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                longAgg,
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
    {
        InternalAggregationFunction longAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                longAgg,
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
    {
        InternalAggregationFunction doubleAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                doubleAgg,
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
    {
        InternalAggregationFunction doubleAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.DOUBLE), parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                doubleAgg,
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
    {
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                stringAgg,
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
    {
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                stringAgg,
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                arrayAgg,
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature("array(bigint)")));
        assertAggregation(
                arrayAgg,
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
    {
        InternalAggregationFunction arrayAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("arbitrary", AGGREGATE, parseTypeSignature("integer"), parseTypeSignature("integer")));
        assertAggregation(
                arrayAgg,
                3,
                createIntsBlock(3, 3, null));
    }
}
