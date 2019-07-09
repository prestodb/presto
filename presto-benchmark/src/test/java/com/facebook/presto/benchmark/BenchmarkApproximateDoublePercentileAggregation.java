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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
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

import static com.facebook.presto.tdigest.StatisticalDigestImplementation.QDIGEST;
import static com.facebook.presto.tdigest.StatisticalDigestImplementation.TDIGEST;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkApproximateDoublePercentileAggregation
{
    @State(Scope.Thread)
    public static class Context
    {
        private static final FeaturesConfig TDIGEST_FEATURES_CONFIG = new FeaturesConfig()
                .setStatisticalDigestImplementation(TDIGEST);

        private static final FeaturesConfig QDIGEST_FEATURES_CONFIG = new FeaturesConfig()
                .setStatisticalDigestImplementation(QDIGEST);

        @Param({"tiny", "sf1", "sf4"})
        String scalingFactor;

        private LocalQueryRunner[] queryRunner = new LocalQueryRunner[2];

        private LocalQueryRunner getQuantileDigestQueryRunner()
        {
            return queryRunner[0];
        }

        private LocalQueryRunner getTDigestQueryRunner()
        {
            return queryRunner[1];
        }

        @Setup
        public void setUp()
                throws IOException
        {
            queryRunner[0] = new LocalQueryRunner(testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema("default")
                    .build(), QDIGEST_FEATURES_CONFIG);
            queryRunner[1] = new LocalQueryRunner(testSessionBuilder()
                    .setCatalog("memory")
                    .setSchema("default")
                    .build(), TDIGEST_FEATURES_CONFIG);

            for (LocalQueryRunner localQueryRunner : queryRunner) {
                localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(), ImmutableMap.of());
                localQueryRunner.createCatalog("memory", new MemoryConnectorFactory(), ImmutableMap.of());
                localQueryRunner.execute(format("CREATE TABLE memory.default.testingtable AS SELECT totalprice from tpch.%s.orders", scalingFactor));
            }
        }

        @TearDown
        public void tearDown()
        {
            for (LocalQueryRunner localQueryRunner : queryRunner) {
                localQueryRunner.close();
                localQueryRunner = null;
            }
        }
    }

    @Benchmark
    public MaterializedResult benchmarkQuantileDigestDoublePercentileAggregation(Context context)
    {
        return context.getQuantileDigestQueryRunner()
                .execute("SELECT approx_percentile(totalprice, 0.5) FROM memory.default.testingtable");
    }

    @Benchmark
    public MaterializedResult benchmarkTDigestDoublePercentileAggregation(Context context)
    {
        return context.getTDigestQueryRunner()
                .execute("SELECT approx_percentile(totalprice, 0.5) FROM memory.default.testingtable");
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkApproximateDoublePercentileAggregation.class.getSimpleName() + ".*")
                .build();

        new Runner(options)
                .run();
    }
}
