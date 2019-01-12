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

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
/*
 * This benchmarks the largest possible search space for the given number of tables.
 * Because of equality inference all tables can be joined with all other table, so there
 * are n! possible orders that don't contain any cross joins.
 */
public class BenchmarkReorderInterconnectedJoins
{
    @Benchmark
    public MaterializedResult benchmarkReorderJoins(BenchmarkInfo benchmarkInfo)
    {
        return benchmarkInfo.getQueryRunner().execute(benchmarkInfo.getQuery());
    }

    @State(Thread)
    public static class BenchmarkInfo
    {
        @Param({"ELIMINATE_CROSS_JOINS", "AUTOMATIC"})
        private String joinReorderingStrategy;

        @Param({"2", "4", "6", "8", "10"})
        private int numberOfTables;

        private String query;
        private LocalQueryRunner queryRunner;

        @Setup
        public void setup()
        {
            checkState(numberOfTables >= 2, "numberOfTables must be >= 2");
            Session session = testSessionBuilder()
                    .setSystemProperty("join_reordering_strategy", joinReorderingStrategy)
                    .setSystemProperty("join_distribution_type", "AUTOMATIC")
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build();
            queryRunner = new LocalQueryRunner(session);
            queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("EXPLAIN SELECT * FROM nation n1");
            for (int i = 2; i <= numberOfTables; i++) {
                stringBuilder.append(format(" JOIN nation n%s ON n%s.nationkey = n%s.nationkey", i, i - 1, i));
            }
            query = stringBuilder.toString();
        }

        public String getQuery()
        {
            return query;
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
                .include(".*" + BenchmarkReorderInterconnectedJoins.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
