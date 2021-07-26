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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.OddsRatioState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

@AggregationFunction
public final class OddsRatioAggregation
{
    private OddsRatioAggregation() {}

    /**
     * odds_ratio(input_col, input_val1, input_val2, output_col, output_val)
     * -> {low_bound: lb , odds_ratio: or , upper_bound:ub}
     * <p>
     * Calculates the ratio of the odds of output_val occurring for input_val1 vs input_val2.
     */
    @InputFunction
    public static void input(
            @AggregationState OddsRatioState state,
            @SqlType(StandardTypes.VARCHAR) Slice inputCol,
            @SqlType(StandardTypes.VARCHAR) Slice inputVal1,
            @SqlType(StandardTypes.VARCHAR) Slice inputVal2,
            @SqlType(StandardTypes.VARCHAR) Slice outputCol,
            @SqlType(StandardTypes.VARCHAR) Slice outputVal)
    {
        if (outputVal.equals(outputCol)) {
            if (inputCol.equals(inputVal1)) {
                state.set11(state.get11() + 1);
            }
            else if (inputCol.equals(inputVal2)) {
                state.set10(state.get10() + 1);
            }
        }
        else {
            if (inputCol.equals(inputVal1)) {
                state.set01(state.get01() + 1);
            }
            else if (inputCol.equals(inputVal2)) {
                state.set00(state.get00() + 1);
            }
        }
    }

    @CombineFunction
    public static void combine(@AggregationState OddsRatioState state, @AggregationState OddsRatioState otherState)
    {
        long n00 = otherState.get00();
        checkArgument(n00 >= 0, "count is negative");

        state.set00(state.get00() + otherState.get00());
        state.set01(state.get01() + otherState.get01());
        state.set10(state.get10() + otherState.get10());
        state.set11(state.get11() + otherState.get11());
    }

    @AggregationFunction(value = "odds_ratio")
    @Description(value = "Returns the odds ratio and a 95% confidence interval")
    @OutputFunction("map(varchar,double)")
    public static void oddsRatio(@AggregationState OddsRatioState state, BlockBuilder out)
    {
        if (state.get00() == 0 || state.get01() == 0 || state.get10() == 0 || state.get11() == 0) {
            out.appendNull();
        }

        double n00 = state.get00();
        double n01 = state.get01();
        double n10 = state.get10();
        double n11 = state.get11();

        double oddsRatio = (n11 * n00) / (n10 * n01);

        double stdErr = 1.96 * Math.sqrt(1 / n00 + 1 / n01 + 1 / n10 + 1 / n11);
        double lnOR = Math.log(oddsRatio);

        // Confidence interval = e^(ln(OR) +/- StandardError)
        double lowerCI = Math.exp(lnOR + stdErr);
        double upperCI = Math.exp(lnOR - stdErr);

        BlockBuilder entryBuilder = out.beginBlockEntry();
        VARCHAR.writeString(entryBuilder, "odds_ratio");
        DOUBLE.writeDouble(entryBuilder, oddsRatio);

        VARCHAR.writeString(entryBuilder, "lower_bound");
        DOUBLE.writeDouble(entryBuilder, lowerCI);

        VARCHAR.writeString(entryBuilder, "upper_bound");
        DOUBLE.writeDouble(entryBuilder, upperCI);
        out.closeEntry();
    }
}
