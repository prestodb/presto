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
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.HashJoinOperator.HashJoinOperatorFactory;
import com.facebook.presto.operator.NullOutputOperator.NullOutputOperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HashBuildAndJoinBenchmark
        extends AbstractOperatorBenchmark
{
    public HashBuildAndJoinBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_build_and_join", 4, 5);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */
    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        // hash build
        BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

        AlignmentOperatorFactory ordersTableScan = new AlignmentOperatorFactory(0, orderOrderKey, totalPrice);
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(1, ordersTableScan.getTupleInfos(), Ints.asList(0), 1_500_000);

        DriverFactory hashBuildDriverFactory = new DriverFactory(true, false, ordersTableScan, hashBuilder);
        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(true, false).addDriverContext());

        // join
        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        AlignmentOperatorFactory lineItemTableScan = new AlignmentOperatorFactory(0, lineItemOrderKey, lineNumber);

        HashJoinOperatorFactory joinOperator = HashJoinOperator.innerJoin(1, hashBuilder.getHashSupplier(), lineItemTableScan.getTupleInfos(), Ints.asList(0));

        NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, joinOperator.getTupleInfos());

        DriverFactory joinDriverFactory = new DriverFactory(true, true, lineItemTableScan, joinOperator, output);
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(true, true).addDriverContext());

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new HashBuildAndJoinBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
