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
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
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

public class HashJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private HashSupplier hashSupplier;

    public HashJoinBenchmark(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hash_join", 4, 5);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        if (hashSupplier == null) {
            BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
            BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);

            AlignmentOperatorFactory ordersTableScan = new AlignmentOperatorFactory(0, orderOrderKey, totalPrice);
            HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(1, ordersTableScan.getTupleInfos(), Ints.asList(0), 1_500_000);

            DriverContext driverContext = taskContext.addPipelineContext(false, false).addDriverContext();
            Driver driver = new DriverFactory(false, false, ordersTableScan, hashBuilder).createDriver(driverContext);
            while (!driver.isFinished()) {
                driver.process();
            }
            hashSupplier = hashBuilder.getHashSupplier();
        }

        BlockIterable lineItemOrderKey = getBlockIterable("lineitem", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable lineNumber = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        AlignmentOperatorFactory lineItemTableScan = new AlignmentOperatorFactory(0, lineItemOrderKey, lineNumber);

        HashJoinOperatorFactory joinOperator = HashJoinOperator.innerJoin(1, hashSupplier, lineItemTableScan.getTupleInfos(), Ints.asList(0));

        NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, joinOperator.getTupleInfos());

        DriverFactory driverFactory = new DriverFactory(true, true, lineItemTableScan, joinOperator, output);
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new HashJoinBenchmark(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
