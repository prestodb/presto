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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkReorderChainedJoins
{
    @Benchmark
    public MaterializedResult benchmarkReorderJoins(BenchmarkInfo benchmarkInfo)
    {
        return benchmarkInfo.getQueryRunner().execute(
                "EXPLAIN SELECT * FROM " +
                        "nation n1 JOIN nation n2 ON n1.nationkey = n2.nationkey " +
                        "JOIN nation n3 ON n2.comment = n3.comment " +
                        "JOIN nation n4 ON n3.name = n4.name " +
                        "JOIN region r1 ON n4.regionkey = r1.regionkey " +
                        "JOIN region r2 ON r1.name = r2.name " +
                        "JOIN region r3 ON r3.comment = r2.comment " +
                        "JOIN region r4 ON r4.regionkey = r3.regionkey");
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        @Param({"ELIMINATE_CROSS_JOINS", "AUTOMATIC"})
        private String joinReorderingStrategy;

        private LocalQueryRunner queryRunner;

        @Setup
        public void setup()
        {
            Session session = testSessionBuilder()
                    .setSystemProperty("join_reordering_strategy", joinReorderingStrategy)
                    .setSystemProperty("join_distribution_type", "AUTOMATIC")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner = new LocalQueryRunner(session);
            queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        }

        public QueryRunner getQueryRunner()
        {
            return queryRunner;
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkReorderChainedJoins.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
