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

import com.facebook.presto.plugin.memory.MemoryConnectorFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkDoubleStatisticalDigestAggregationFunctions
{
    @State(Scope.Thread)
    public static class Context
    {
        @Param({"1", "2", "3"})
        int scalingFactor;

        private LocalQueryRunner queryRunner;

        public LocalQueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @Setup
        public void setUp()
                throws IOException
        {
            queryRunner = new LocalQueryRunner(testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema("default")
                    .build());
            queryRunner.createCatalog("tpch", new TpchConnectorFactory(), ImmutableMap.of());
            queryRunner.createCatalog("memory", new MemoryConnectorFactory(), ImmutableMap.of());

            queryRunner.execute(format("CREATE TABLE memory.default.testingtable AS SELECT totalprice from tpch.sf%s.orders", scalingFactor));
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
        }
    }

    @Benchmark
    public MaterializedResult benchmarkTDigestAggregation(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT tdigest_agg(totalprice) FROM memory.default.testingtable");
    }

    @Benchmark
    public MaterializedResult benchmarkQuantileDigestAggregation(Context context)
    {
        return context.getQueryRunner()
                .execute("SELECT qdigest_agg(totalprice) FROM memory.default.testingtable");
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkDoubleStatisticalDigestAggregationFunctions.class.getSimpleName() + ".*")
                .build();

        new Runner(options)
                .run();
    }
}
