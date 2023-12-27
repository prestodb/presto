package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.DoubleState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction(value = "sum_if", isCalledOnNullInput = true)
public final class SumIfAggregation
{
    private SumIfAggregation() {}

    @InputFunction
    public static void input(@AggregationState DoubleState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.DOUBLE) double addend)
    {
        if (value) {
            state.setDouble(state.getDouble() + addend);
        }
    }

    @InputFunction
    public static void input(@AggregationState DoubleState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.DOUBLE) double addend, @SqlType(StandardTypes.DOUBLE) double fallback)
    {
        if (value) {
            state.setDouble(state.getDouble() + addend);
        }
        else {
            state.setDouble(state.getDouble() + fallback);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState DoubleState state, @AggregationState DoubleState otherState)
    {
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState DoubleState state, BlockBuilder out)
    {
        DOUBLE.writeDouble(out, state.getDouble());
    }
}
