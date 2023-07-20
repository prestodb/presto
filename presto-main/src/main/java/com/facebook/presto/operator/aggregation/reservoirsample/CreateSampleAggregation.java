package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.*;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyState;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.operator.aggregation.differentialentropy.UnweightedReservoirSampleStateStrategy;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.sql.tree.Row;

@AggregationFunction("create_sample")
public class CreateSampleAggregation {
    private CreateSampleAggregation() {}


    @InputFunction
    public static void input(
            @AggregationState DifferentialEntropyState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.ROW)  RowType sample
            )
    {
        ReservoirSampleAllColsStateStrategy strategy = ReservoirSampleAllColsStateStrategy.getStrategy(
                (ReservoirSampleAllColsStateStrategy) state.getStrategy(),
                size);
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
        assert strategy instanceof ReservoirSampleAllColsStateStrategy;
        DifferentialEntropyStateStrategy.combine(strategy, otherStrategy);
    }

    @OutputFunction("array(row)")
//    @OutputFunction("double")
    public static void output(@AggregationState DifferentialEntropyState state, BlockBuilder out)
    {
        RowType[] samples = ((ReservoirSampleAllColsStateStrategy) state.getStrategy()).getReservoir().getSamples();
        BlockBuilder entryBuilder = out.beginBlockEntry();
        for (RowType x: samples) {
            x.writeObject(entryBuilder, x);
        }
        out.closeEntry();
        // todo: needs to create a new type to write the samples to the block
//        DOUBLE.writeDouble(out, samples.length == 0 ? Double.NaN : samples[0]);
    }
}
