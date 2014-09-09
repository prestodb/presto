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
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static org.testng.Assert.assertNotNull;

public class TestMaxByAggregation
{
    private static final MetadataManager metadata = new MetadataManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = FluentIterable.from(metadata.getTypeManager().getTypes()).filter(new Predicate<Type>() {
            @Override
            public boolean apply(Type input)
            {
                return input.isOrderable();
            }
        }).toSet();

        for (Type keyType : orderableTypes) {
            for (Type valueType : metadata.getTypeManager().getTypes()) {
                assertNotNull(metadata.getExactFunction(new Signature("max_by", valueType.getName(), valueType.getName(), keyType.getName())));
            }
        }
    }

    @Test
    public void testNull()
            throws Exception
    {
        InternalAggregationFunction doubleDouble = metadata.getExactFunction(new Signature("max_by", DoubleType.NAME, DoubleType.NAME, DoubleType.NAME)).getAggregationFunction();
        assertAggregation(
                doubleDouble,
                1.0,
                null,
                createPage(
                        new Double[] {1.0, null},
                        new Double[] {1.0, 2.0}));
    }

    @Test
    public void testDoubleDouble()
            throws Exception
    {
        InternalAggregationFunction doubleDouble = metadata.getExactFunction(new Signature("max_by", DoubleType.NAME, DoubleType.NAME, DoubleType.NAME)).getAggregationFunction();
        assertAggregation(
                doubleDouble,
                1.0,
                null,
                createPage(
                        new Double[] {null},
                        new Double[] {null}),
                createPage(
                        new Double[] {null},
                        new Double[] {null}));

        assertAggregation(
                doubleDouble,
                1.0,
                2.0,
                createPage(
                        new Double[] {3.0, 2.0},
                        new Double[] {1.0, 1.5}),
                createPage(
                        new Double[] {null},
                        new Double[] {null}));
    }

    @Test
    public void testDoubleVarchar()
            throws Exception
    {
        InternalAggregationFunction doubleVarchar = metadata.getExactFunction(new Signature("max_by", VarcharType.NAME, VarcharType.NAME, DoubleType.NAME)).getAggregationFunction();
        assertAggregation(
                doubleVarchar,
                1.0,
                "a",
                createPage(
                        new String[] {"z", "a"},
                        new Double[] {1.0, 2.0}),
                createPage(
                        new String[] {null},
                        new Double[] {null}));

        assertAggregation(
                doubleVarchar,
                1.0,
                "hi",
                createPage(
                        new String[] {"zz", "hi"},
                        new Double[] {0.0, 1.0}),
                createPage(
                        new String[] {null, "a"},
                        new Double[] {null, -1.0}));
    }

    private static Page createPage(Double[] values, Double[] keys)
    {
        return new Page(createDoublesBlock(values), createDoublesBlock(keys));
    }

    private static Page createPage(String[] values, Double[] keys)
    {
        return new Page(createStringsBlock(values), createDoublesBlock(keys));
    }
}
