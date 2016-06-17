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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createFloatsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Float.floatToRawIntBits;

@Test(singleThreaded = true)
public class TestFloatAverageAggregation
        extends AbstractTestAggregationFunction
{
    private InternalAggregationFunction avgFunction;

    @BeforeMethod
    public void setUp()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        avgFunction = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("avg", FunctionKind.AGGREGATE, parseTypeSignature(StandardTypes.FLOAT), parseTypeSignature(StandardTypes.FLOAT)));
    }

    @Test
    public void averageOfNullIsNull()
    {
        assertAggregation(avgFunction,
                1.0,
                null,
                createFloatsBlock(null, null));
    }

    @Test
    public void averageOfSingleValueEqualsThatValue()
    {
        assertAggregation(avgFunction,
                1.0,
                1.23f,
                createFloatsBlock(1.23f));
    }

    @Test
    public void averageOfTwoMaxFloatsEqualsMaxFloat()
    {
        assertAggregation(avgFunction,
                1.0,
                Float.MAX_VALUE,
                createFloatsBlock(Float.MAX_VALUE, Float.MAX_VALUE));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = FLOAT.createBlockBuilder(new BlockBuilderStatus(), length);
        for (int i = start; i < start + length; i++) {
            FLOAT.writeLong(blockBuilder, floatToRawIntBits((float) i));
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
        return ImmutableList.of(StandardTypes.FLOAT);
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
