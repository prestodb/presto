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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestDoubleMaxAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "max";
    private JavaAggregationFunctionImplementation maxFunction;

    @BeforeClass
    public void setup()
    {
        FunctionAndTypeManager functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
        maxFunction = functionAndTypeManager.getJavaAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction(TestDoubleMaxAggregation.FUNCTION_NAME, fromTypes(DOUBLE)));
    }

    @Test
    public void max()
    {
        assertAggregation(maxFunction, 4.0, createDoublesBlock(4.0));
        assertAggregation(maxFunction, 5.0, createDoublesBlock(2.0, 4.0, 5.0));
        assertAggregation(maxFunction, 4.0, createDoublesBlock(4.0, NaN, null));
        assertAggregation(maxFunction, 4.0, createDoublesBlock(NaN, 4.0, null));
        assertAggregation(maxFunction, POSITIVE_INFINITY, createDoublesBlock(NaN, null, POSITIVE_INFINITY));
        assertAggregation(maxFunction, POSITIVE_INFINITY, createDoublesBlock(NaN, null, NEGATIVE_INFINITY, POSITIVE_INFINITY));
        assertAggregation(maxFunction, null, createDoublesBlock((Double) null));
        assertAggregation(maxFunction, NaN, createDoublesBlock(NaN));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, (double) i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return (double) (start + length - 1);
    }

    @Override
    protected String getFunctionName()
    {
        return "max";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE);
    }
}
