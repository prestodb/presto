package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;

public class DoubleSumAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public DoubleSumAggregationBenchmark()
    {
        super("double_sum_agg", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable blockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(blockIterable);
        return new AggregationOperator(alignmentOperator, Step.SINGLE, ImmutableList.of(aggregation(DOUBLE_SUM, new Input(0,0))));
    }

    public static void main(String[] args)
    {
        new DoubleSumAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
