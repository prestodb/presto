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
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.block.BlockIterables.concat;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class OrderByBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    private static final int ROWS = 1_500_000;

    public OrderByBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "in_memory_orderby_1.5M", 5, 10);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        BlockIterable clerk = getBlockIterable("orders", "clerk", BlocksFileEncoding.RAW);

        AlignmentOperatorFactory alignmentOperator = new AlignmentOperatorFactory(0, concat(nCopies(100, totalPrice)), concat(nCopies(100, clerk)));

        LimitOperatorFactory limitOperator = new LimitOperatorFactory(1, alignmentOperator.getTupleInfos(), ROWS);

        OrderByOperatorFactory orderByOperator = new OrderByOperatorFactory(
                2,
                limitOperator.getTupleInfos(),
                new int[] {0},
                new int[] {1},
                ROWS);

        return ImmutableList.of(alignmentOperator, limitOperator, orderByOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new OrderByBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
