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

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Set;

import org.testng.annotations.Test;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.FluentIterable;

public class TestArrayAggregation
{
    private static final MetadataManager metadata = new MetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> allTypes = FluentIterable.from(metadata.getTypeManager().getTypes()).toSet();

        for (Type valueType : allTypes) {
            String retType = "array<" + valueType.getDisplayName() + ">";
            assertNotNull(metadata.getExactFunction(new Signature("array_agg", retType, valueType.getDisplayName())));
        }
    }

    @Test
    public void testBoolean()
        throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getExactFunction(new Signature("array_agg", "array<boolean>", "boolean")).getAggregationFunction();
        assertAggregation(
                booleanAgg,
                1.0,
                Arrays.asList(true, false),
                new Page(createBooleansBlock(new Boolean[] {true, false})));
    }

    @Test
    public void testBigInt()
        throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getExactFunction(new Signature("array_agg", "array<bigint>", "bigint")).getAggregationFunction();
        assertAggregation(
                bigIntAgg,
                1.0,
                Arrays.asList(2L, 1L, 2L),
                new Page(createLongsBlock(new Long[] {2L, 1L, 2L})));
    }

    @Test
    public void testVarchar()
        throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getExactFunction(new Signature("array_agg", "array<varchar>", "varchar")).getAggregationFunction();
        assertAggregation(
                varcharAgg,
                1.0,
                Arrays.asList("hello", "world"),
                new Page(createStringsBlock(new String[] {"hello", "world"})));
    }
}
