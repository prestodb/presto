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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
public class BenchmarkIcebergLazyLoading
{
    @Language("SQL") final String optimizationQuery = "select min(a), max(a), min(b), max(b) from iceberg_partition";

    @Param({"300 * 2", "1600 * 4"})
    private String recordCount = "300 * 2";

    DistributedQueryRunner queryRunner;
    int batchCount;
    int countPerBatch;
    @Language("SQL") String normalQuery;

    @Setup
    public void setup() throws Exception
    {
        queryRunner = IcebergQueryRunner.builder()
                .build()
                .getQueryRunner();
        queryRunner.execute("create table iceberg_partition(a int, b int, c double) with (partitioning = ARRAY['a', 'b'])");

        String[] batchAndPerBatch = recordCount.split("\\*");
        batchCount = Integer.parseInt(batchAndPerBatch[0].trim());
        countPerBatch = Integer.parseInt(batchAndPerBatch[1].trim());
        normalQuery = String.format("select a, c from iceberg_partition where b >= %s", countPerBatch - 1);

        for (int b = 0; b < batchCount; b++) {
            StringBuilder sqlBuilder = new StringBuilder(String.format("values (%d, %d, %d)", b, b, b * b));
            for (int i = 1; i < countPerBatch; i++) {
                sqlBuilder.append(String.format(", (%d, %d, %d)", b, i, b * i));
            }
            String valuesSql = sqlBuilder.toString();
            queryRunner.execute("insert into iceberg_partition " + valuesSql);
        }
    }

    @Benchmark
    public void testFurtherOptimize(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(optimizationQuery);
        bh.consume(result.getRowCount());
    }

    @Benchmark
    public void testNormalQuery(Blackhole bh)
    {
        MaterializedResult result = queryRunner.execute(normalQuery);
        bh.consume(result.getRowCount());
    }

    @TearDown
    public void finish()
    {
        queryRunner.execute("drop table iceberg_partition");
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.INDI)
                .include(".*" + BenchmarkIcebergLazyLoading.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
