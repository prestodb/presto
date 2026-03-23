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
package com.facebook.presto.hive.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.airlift.tpch.TpchTable.getTables;
import static java.util.Objects.requireNonNull;

/**
 * Runs SQL benchmarks against a Hive-backed DistributedQueryRunner.
 * Supports comparing multiple session configurations side by side.
 *
 * <p>Usage:
 * <pre>
 * try (HiveDistributedBenchmarkRunner runner = new HiveDistributedBenchmarkRunner(3, 5)) {
 *     runner.addScenario("baseline", sessionBuilder -> {});
 *     runner.addScenario("optimized", sessionBuilder ->
 *             sessionBuilder.setSystemProperty("my_property", "true"));
 *     runner.run("SELECT ... GROUP BY CUBE(...)");
 * }
 * </pre>
 */
public class HiveDistributedBenchmarkRunner
        implements AutoCloseable
{
    private final QueryRunner queryRunner;
    private final int warmupIterations;
    private final int measuredIterations;
    private final Map<String, Session> scenarios = new LinkedHashMap<>();
    private final StringBuilder results = new StringBuilder();

    public HiveDistributedBenchmarkRunner(int warmupIterations, int measuredIterations)
            throws Exception
    {
        this.warmupIterations = warmupIterations;
        this.measuredIterations = measuredIterations;
        this.queryRunner = HiveQueryRunner.createQueryRunner(getTables());
    }

    public void addScenario(String name, SessionConfigurator configurator)
    {
        requireNonNull(name, "name is null");
        requireNonNull(configurator, "configurator is null");
        Session.SessionBuilder builder = Session.builder(queryRunner.getDefaultSession());
        configurator.configure(builder);
        scenarios.put(name, builder.build());
    }

    public String run(String sql)
    {
        results.setLength(0);
        Map<String, Long> averages = new LinkedHashMap<>();

        for (Map.Entry<String, Session> entry : scenarios.entrySet()) {
            String name = entry.getKey();
            Session session = entry.getValue();
            long avg = runScenario(name, session, sql);
            averages.put(name, avg);
        }

        // Summary
        results.append("\n=== Summary ===\n");
        Long baselineAvg = averages.values().iterator().next();
        for (Map.Entry<String, Long> entry : averages.entrySet()) {
            double speedup = (double) baselineAvg / entry.getValue();
            results.append(String.format("%-30s %6d ms  (%.2fx)\n",
                    entry.getKey(), entry.getValue(), speedup));
        }

        String output = results.toString();
        System.out.println(output);

        // Write to file since surefire mixes stdout with logging
        try {
            String path = System.getProperty("java.io.tmpdir") + "/hive_benchmark_results.txt";
            java.nio.file.Files.write(java.nio.file.Paths.get(path), output.getBytes());
            System.out.println("Results written to: " + path);
        }
        catch (Exception e) {
            // ignore
        }

        return output;
    }

    /**
     * Runs the benchmark query with correctness verification.
     * All scenarios must produce the same results as the first scenario.
     */
    public String runWithVerification(String sql)
    {
        String output = run(sql);

        // Verify correctness: all scenarios must match the first
        MaterializedResult expected = null;
        for (Map.Entry<String, Session> entry : scenarios.entrySet()) {
            MaterializedResult actual = queryRunner.execute(entry.getValue(), sql);
            if (expected == null) {
                expected = actual;
            }
            else {
                if (!resultsMatch(expected, actual)) {
                    throw new AssertionError(
                            "Results mismatch for scenario '" + entry.getKey() + "'");
                }
            }
        }
        return output;
    }

    private long runScenario(String name, Session session, String sql)
    {
        results.append(String.format("--- %s ---\n", name));

        // Warmup
        for (int i = 0; i < warmupIterations; i++) {
            queryRunner.execute(session, sql);
        }

        // Measured runs
        long totalMs = 0;
        for (int i = 0; i < measuredIterations; i++) {
            long start = System.nanoTime();
            queryRunner.execute(session, sql);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            totalMs += elapsedMs;
            results.append(String.format("  run %d: %d ms\n", i + 1, elapsedMs));
        }
        long avg = totalMs / measuredIterations;
        results.append(String.format("  avg: %d ms\n\n", avg));
        return avg;
    }

    private static boolean resultsMatch(MaterializedResult a, MaterializedResult b)
    {
        return a.getMaterializedRows().size() == b.getMaterializedRows().size()
                && new java.util.HashSet<>(a.getMaterializedRows())
                        .equals(new java.util.HashSet<>(b.getMaterializedRows()));
    }

    public QueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }

    @FunctionalInterface
    public interface SessionConfigurator
    {
        void configure(Session.SessionBuilder builder);
    }
}
