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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.FluentIterable;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> allTypes = FluentIterable.from(metadata.getTypeManager().getTypes()).toSet();

        for (Type valueType : allTypes) {
            assertNotNull(metadata.getExactFunction(new Signature("arbitrary", valueType.getTypeSignature(), valueType.getTypeSignature())));
        }
    }

    @Test
    public void testNullBoolean()
        throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.BOOLEAN, StandardTypes.BOOLEAN)).getAggregationFunction();
        assertAggregation(
                booleanAgg,
                1.0,
                null,
                createPage(
                        new Boolean[] {null}));
    }

    @Test
    public void testValidBoolean()
            throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.BOOLEAN, StandardTypes.BOOLEAN)).getAggregationFunction();
        assertAggregation(
                booleanAgg,
                1.0,
                true,
                createPage(
                        new Boolean[] {true, true}));
    }

    @Test
    public void testNullLong()
            throws Exception
    {
        InternalAggregationFunction longAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.BIGINT, StandardTypes.BIGINT)).getAggregationFunction();
        assertAggregation(
                longAgg,
                1.0,
                null,
                createPage(
                        new Long[] {null, null}));
    }

    @Test
    public void testValidLong()
            throws Exception
    {
        InternalAggregationFunction longAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.BIGINT, StandardTypes.BIGINT)).getAggregationFunction();
        assertAggregation(
                longAgg,
                1.0,
                1L,
                createPage(
                        new Long[] {1L , null}));
    }

    @Test
    public void testNullDouble()
            throws Exception
    {
        InternalAggregationFunction doubleAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.DOUBLE, StandardTypes.DOUBLE)).getAggregationFunction();
        assertAggregation(
                doubleAgg,
                1.0,
                null,
                createPage(
                        new Double[] {null, null}));
    }

    @Test
    public void testValidDouble()
            throws Exception
    {
        InternalAggregationFunction doubleAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.DOUBLE, StandardTypes.DOUBLE)).getAggregationFunction();
        assertAggregation(
                doubleAgg,
                1.0,
                2.0,
                createPage(
                        new Double[] {null, 2.0}));
    }

    @Test
    public void testNullString()
            throws Exception
    {
        InternalAggregationFunction stringAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.VARCHAR, StandardTypes.VARCHAR)).getAggregationFunction();
        assertAggregation(
                stringAgg,
                1.0,
                null,
                createPage(
                        new String[] {null, null}));
    }

    @Test
    public void testValidString()
            throws Exception
    {
        InternalAggregationFunction stringAgg = metadata.getExactFunction(new Signature("arbitrary", StandardTypes.VARCHAR, StandardTypes.VARCHAR)).getAggregationFunction();
        assertAggregation(
                stringAgg,
                1.0,
                "a",
                createPage(
                        new String[] {"a", "a"}));
    }

    private static Page createPage(Boolean[] values)
    {
        return new Page(createBooleansBlock(values));
    }

    private static Page createPage(Long[] values)
    {
        return new Page(createLongsBlock(values));
    }

    private static Page createPage(Double[] values)
    {
        return new Page(createDoublesBlock(values));
    }

    private static Page createPage(String[] values)
    {
        return new Page(createStringsBlock(values));
    }
}
