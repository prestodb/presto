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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.facebook.presto.spi.type.StandardTypes;
import org.testng.annotations.Test;
import java.util.List;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestGeometricAggregationFunction
{
    protected final TypeRegistry typeRegistry = new TypeRegistry();
    protected final FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), true);

    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), length);
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, (double) i);
        }
        return new Block[] {blockBuilder.build()};
    }

    protected String getFunctionName()
    {
        return "gavg";
    }

    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        double product = 1.0d;
        for (int i = start; i < start + length; i++) {
            product *= i;
        }
        return Math.pow(product, 1.0d / length);
    }

    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE);
    }

    protected final InternalAggregationFunction getFunction()
    {
        return functionRegistry.resolveFunction(QualifiedName.of(getFunctionName()), Lists.transform(getFunctionParameterTypes(), TypeSignature::parseTypeSignature), false).getAggregationFunction();
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlocks(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlocks(2, 4));
    }

    protected void testAggregation(Object expectedValue, Block... blocks)
    {
        assertAggregation(getFunction(), 1d, expectedValue, blocks[0].getPositionCount(), blocks);
    }
}
