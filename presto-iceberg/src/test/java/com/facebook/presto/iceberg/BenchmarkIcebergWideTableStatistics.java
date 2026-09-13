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
import java.util.stream.IntStream;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

/**
 * Measures planning against a wide table, where folding the manifest summary dominates.
 *
 * <p>Deserializing the per-file lower bounds, upper bounds and null counts costs one map entry per
 * column per data file, so on a 500 column table it swamps the rest of the manifest read. These
 * benchmarks EXPLAIN rather than execute, because that cost is paid during planning: the statistics
 * are requested while the optimizer is still costing the scan.
 *
 * <p>The three cases differ only in how many columns the query needs statistics for, which is what
 * bounds the request:
 *
 * <ul>
 *   <li>{@link #explainProjectingTwentyColumns} - 20 of 500, the shape this exists to make cheap
 *   <li>{@link #explainProjectingAllColumns} - all 500, the cost when every column is genuinely
 *       needed, so the delta is visible in a single run without reverting anything
 *   <li>{@link #explainCountStar} - no columns at all, which skips the bounds request entirely
 * </ul>
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkIcebergWideTableStatistics
{
    private static final String TABLE_NAME = "iceberg_wide_500";
    private static final int COLUMN_COUNT = 500;
    private static final int PROJECTED_COLUMN_COUNT = 20;
    // One data file per insert. The bounds decode scales with columns times files, so the file count
    // matters and the row count does not.
    private static final int FILE_COUNT = 20;
    private static final int ROWS_PER_FILE = 100;

    DistributedQueryRunner queryRunner;
    Session session;

    @Setup
    public void setup()
            throws Exception
    {
        queryRunner = IcebergQueryRunner.builder()
                .build()
                .getQueryRunner();
        session = queryRunner.getDefaultSession();

        queryRunner.execute(session, format("create table %s (%s)", TABLE_NAME, IntStream.range(0, COLUMN_COUNT)
                .mapToObj(column -> format("c%s bigint", column))
                .collect(joining(", "))));

        for (int file = 0; file < FILE_COUNT; file++) {
            // Offsetting every column by a different amount per file keeps each file's bounds
            // distinct, so folding min/max across files cannot be short-circuited.
            int offset = file * COLUMN_COUNT;
            queryRunner.execute(session, format("insert into %s select %s from tpch.tiny.orders limit %s",
                    TABLE_NAME,
                    IntStream.range(0, COLUMN_COUNT)
                            .mapToObj(column -> format("orderkey + %s", column + offset))
                            .collect(joining(", ")),
                    ROWS_PER_FILE));
        }
    }

    @Benchmark
    public void explainProjectingTwentyColumns(Blackhole blackhole)
    {
        blackhole.consume(explainJoinProjecting(PROJECTED_COLUMN_COUNT));
    }

    @Benchmark
    public void explainProjectingAllColumns(Blackhole blackhole)
    {
        blackhole.consume(explainJoinProjecting(COLUMN_COUNT));
    }

    @Benchmark
    public void explainCountStar(Blackhole blackhole)
    {
        MaterializedResult result = queryRunner.execute(session, format("explain select count(*) from %s", TABLE_NAME));
        blackhole.consume(result.getRowCount());
    }

    /**
     * Joins the wide table so that the optimizer has to cost both sides, which is what makes it ask
     * the connector for statistics on the projected and predicate columns.
     */
    private int explainJoinProjecting(int columnCount)
    {
        String projection = IntStream.range(0, columnCount)
                .mapToObj(column -> format("wide.c%s", column))
                .collect(joining(", "));
        MaterializedResult result = queryRunner.execute(session, format(
                "explain select %s from %s wide join tpch.tiny.orders orders on wide.c0 = orders.orderkey where wide.c1 > 10",
                projection,
                TABLE_NAME));
        return result.getRowCount();
    }

    @TearDown
    public void finish()
    {
        queryRunner.execute(session, format("drop table if exists %s", TABLE_NAME));
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkIcebergWideTableStatistics.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
