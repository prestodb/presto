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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestDoubleVariancePopAggregation
        extends AbstractTestAggregationFunction
{
    protected final MetadataManager metadata = new MetadataManager();

    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            blockBuilder.appendDouble((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public InternalAggregationFunction getFunction()
    {
        return metadata.resolveFunction(new QualifiedName("var_pop"), ImmutableList.<Type>of(DOUBLE), false).getAggregationFunction();
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }

        Variance variance = new Variance(false);
        return variance.evaluate(values);
    }
}
