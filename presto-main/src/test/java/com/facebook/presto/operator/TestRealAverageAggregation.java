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
package com.facebook.presto.operator;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Float.floatToRawIntBits;

@Test(singleThreaded = true)
public class TestRealAverageAggregation
        extends AbstractTestAggregationFunction
{
    private InternalAggregationFunction avgFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionAndTypeManager functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
        avgFunction = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("avg", fromTypes(REAL)));
    }

    @Test
    public void averageOfNullIsNull()
    {
        assertAggregation(avgFunction,
                null,
                createBlockOfReals(null, null));
    }

    @Test
    public void averageOfSingleValueEqualsThatValue()
    {
        assertAggregation(avgFunction,
                1.23f,
                createBlockOfReals(1.23f));
    }

    @Test
    public void averageOfTwoMaxFloatsEqualsMaxFloat()
    {
        assertAggregation(avgFunction,
                Float.MAX_VALUE,
                createBlockOfReals(Float.MAX_VALUE, Float.MAX_VALUE));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            REAL.writeLong(blockBuilder, floatToRawIntBits((float) i));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "avg";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.REAL);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        float sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }
        return sum / length;
    }
}
