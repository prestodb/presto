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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkIcebergAggregatePushDown
{
    DistributedQueryRunner queryRunner;
    Session writerSession;
    Session aggregatePushDownEnabledSession;
    Session aggregatePushDownDisabledSession;

    @Setup
    public void setup() throws Exception
    {
        queryRunner = IcebergQueryRunner.builder()
                .build()
                .getQueryRunner();
        writerSession = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty("iceberg", "max_partitions_per_writer", "1000")
                .build();
        queryRunner.execute(writerSession, "create table iceberg_lineitem with (partitioning = ARRAY['suppkey']) " +
                "as select * from tpch.sf1.lineitem with no data");
        for (int i = 0; i < 10; i++) {
            queryRunner.execute(writerSession, "insert into iceberg_lineitem " +
                    "select * from tpch.sf1.lineitem where suppkey > " + i * 1000 + " and suppkey <= " + (i + 1) * 1000);
        }

        aggregatePushDownEnabledSession = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty("iceberg", "aggregate_push_down_enabled", "true")
                .build();
        aggregatePushDownDisabledSession = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty("iceberg", "aggregate_push_down_enabled", "false")
                .build();
    }

    @Benchmark
    public void aggregatePushDownEnabledQuery(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(aggregatePushDownEnabledSession,
                "select count(*), min(orderkey), max(orderkey), min(commitdate), max(commitdate)" +
                        " from iceberg_lineitem");
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void aggregatePushDownDisabledQuery(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(aggregatePushDownDisabledSession,
                "select count(*), min(orderkey), max(orderkey), min(commitdate), max(commitdate)" +
                        " from iceberg_lineitem");
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void aggregatePushDownEnabledQueryWithFilter(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(aggregatePushDownEnabledSession,
                "select count(*), min(orderkey), max(orderkey), min(commitdate), max(commitdate)" +
                        " from iceberg_lineitem" +
                        " where suppkey > 1000 and suppkey <= 9000");
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void aggregatePushDownDisabledQueryWithFilter(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(aggregatePushDownDisabledSession,
                "select count(*), min(orderkey), max(orderkey), min(commitdate), max(commitdate)" +
                        " from iceberg_lineitem" +
                        " where suppkey > 1000 and suppkey <= 9000");
        bh.consume(result.getRowCount());
    }

    @TearDown
    public void finish()
    {
        queryRunner.execute("drop table iceberg_lineitem");
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkIcebergAggregatePushDown.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
