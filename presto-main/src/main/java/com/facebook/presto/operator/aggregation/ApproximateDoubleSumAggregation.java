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

import com.facebook.presto.operator.aggregation.state.VarianceState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.sumError;
import static com.facebook.presto.operator.aggregation.AggregationUtils.mergeVarianceState;
import static com.facebook.presto.operator.aggregation.AggregationUtils.updateVarianceState;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class ApproximateDoubleSumAggregation
        extends AbstractApproximateAggregationFunction<ApproximateDoubleSumAggregation.ApproximateDoubleSumState>
{
    public static final ApproximateDoubleSumAggregation DOUBLE_APPROXIMATE_SUM_AGGREGATION = new ApproximateDoubleSumAggregation();

    public ApproximateDoubleSumAggregation()
    {
        // TODO: Change intermediate to fixed width, once we have a better type system
        super(VARCHAR, VARCHAR, DOUBLE);
    }

    public interface ApproximateDoubleSumState
            extends VarianceState
    {
        double getSum();

        void setSum(double value);

        long getWeightedCount();

        void setWeightedCount(long value);
    }

    @Override
    protected void processInput(ApproximateDoubleSumState state, BlockCursor cursor, long sampleWeight)
    {
        double value = cursor.getDouble();

        state.setWeightedCount(state.getWeightedCount() + sampleWeight);
        state.setSum(state.getSum() + value * sampleWeight);
        updateVarianceState(state, value);
    }

    @Override
    protected void combineState(ApproximateDoubleSumState state, ApproximateDoubleSumState otherState)
    {
        state.setSum(state.getSum() + otherState.getSum());
        state.setWeightedCount(state.getWeightedCount() + otherState.getWeightedCount());
        mergeVarianceState(state, otherState);
    }

    @Override
    protected void evaluateFinal(ApproximateDoubleSumState state, double confidence, BlockBuilder out)
    {
        if (state.getWeightedCount() == 0) {
            out.appendNull();
            return;
        }

        String result = formatApproximateResult(
                state.getSum(),
                sumError(state.getCount(), state.getWeightedCount(), state.getM2(), state.getMean()),
                confidence,
                false);
        out.appendSlice(Slices.utf8Slice(result));
    }
}
