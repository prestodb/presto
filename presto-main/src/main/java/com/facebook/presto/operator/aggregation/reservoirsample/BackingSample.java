package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.PrecisionRecallAggregation;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyState;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.UnweightedReservoirSampleStateStrategy;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.function.*;

import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("create_sample")
public class BackingSample {
    private BackingSample() {}


    @InputFunction
    public static void input(
            @AggregationState DifferentialEntropyState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                size,
                sample);
        state.setStrategy(strategy);
        strategy.add(sample);
    }

    @CombineFunction
    public static void combine(@AggregationState DifferentialEntropyState state, @AggregationState DifferentialEntropyState otherState)
    {
        DifferentialEntropyStateStrategy strategy = state.getStrategy();
        DifferentialEntropyStateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy);
            return;
        }
        if (otherStrategy == null) {
            return;
        }
        DifferentialEntropyStateStrategy.combine(strategy, otherStrategy);
    }

    @OutputFunction("array(row)")
//    @OutputFunction("double")
    public static void output(@AggregationState DifferentialEntropyState state, BlockBuilder out)
    {
        double[] samples = ((UnweightedReservoirSampleStateStrategy) state.getStrategy()).getReservoir().getSamples();
        BlockBuilder entryBuilder = out.beginBlockEntry();
        for (double x: samples) {
            DoubleType.DOUBLE.writeDouble(
                    entryBuilder, x);
        }
        out.closeEntry();
        // todo: needs to create a new type to write the samples to the block
//        DOUBLE.writeDouble(out, samples.length == 0 ? Double.NaN : samples[0]);
    }
}
