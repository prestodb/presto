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
package com.facebook.presto.statistics;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.type.AbstractTestType;

import static com.facebook.presto.statistics.GaussianType.GAUSSIAN;

public class TestGaussianType
        extends AbstractTestType
{
    public TestGaussianType()
    {
        super(GAUSSIAN, SqlVarbinary.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = GAUSSIAN.createBlockBuilder(null, 4);
        GAUSSIAN.writeSlice(blockBuilder, new Gaussian(1.0, new double[] {0.0}, new double[] {1.0}).serialize());
        GAUSSIAN.writeSlice(blockBuilder, new Gaussian(10.0, new double[] {1.0, 2.0}, new double[] {3.0, 0.5, 4.0}).serialize());
        GAUSSIAN.writeSlice(blockBuilder, new Gaussian(10.0, new double[] {1.0, 2.0}, new double[] {3.0, 0.5, 4.0}).serialize());
        GAUSSIAN.writeSlice(blockBuilder, new Gaussian(0.25, new double[] {-1.0, 0.0, 1.0}, new double[] {1.0, 0.0, 0.0, 1.0, 0.0, 1.0}).serialize());
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException("Gaussian is not an orderable type");
    }
}
