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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.AbstractTestAggregationFunction;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.block.BlockAssertions.createBlockOfReals;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Float.floatToRawIntBits;

@Test(singleThreaded = true)
public class TestRealAverageAggregation
        extends AbstractTestAggregationFunction
{
    private InternalAggregationFunction avgFunction;

    @BeforeClass
    public void setUp()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        avgFunction = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("avg", FunctionKind.AGGREGATE, parseTypeSignature(StandardTypes.REAL), parseTypeSignature(StandardTypes.REAL)));
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
