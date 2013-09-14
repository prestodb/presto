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

import com.facebook.presto.block.BlockBuilder;
import io.airlift.slice.Slice;

public class LongStdDevAggregation
        extends LongVarianceAggregation
{
    public static final LongStdDevAggregation STDDEV_INSTANCE = new LongStdDevAggregation(false);
    public static final LongStdDevAggregation STDDEV_POP_INSTANCE = new LongStdDevAggregation(true);

    LongStdDevAggregation(boolean population)
    {
        super(population);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        Double result = AbstractVarianceAggregation.buildFinalStdDev(population, valueSlice, valueOffset);

        if (result == null) {
            output.appendNull();
        }
        else {
            output.append(result.doubleValue());
        }
    }
}

