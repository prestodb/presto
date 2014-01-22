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
package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator.AlignmentOperatorFactory;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public HashAggregationBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_agg", 5, 25);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        BlockIterable orderStatusBlockIterable = getBlockIterable("orders", "orderstatus", BlocksFileEncoding.RAW);
        BlockIterable totalPriceBlockIterable = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        AlignmentOperatorFactory alignmentOperator = new AlignmentOperatorFactory(0, orderStatusBlockIterable, totalPriceBlockIterable);
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(1,
                ImmutableList.of(alignmentOperator.getTupleInfos().get(0)),
                Ints.asList(0),
                Step.SINGLE,
                ImmutableList.of(aggregation(DOUBLE_SUM, new Input(1))),
                100_000);
        return ImmutableList.of(alignmentOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new HashAggregationBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
