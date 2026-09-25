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

import org.testng.annotations.Test;

/**
 * Benchmarks the PushProjectionThroughCrossJoin optimization.
 *
 * <p>Uses a real CROSS JOIN between lineitem and a subquery so that the
 * plan produces a JoinNode with isCrossJoin(). CROSS JOIN UNNEST produces
 * an UnnestNode instead, which this rule does not target.
 *
 * <p>Run via:
 * <pre>
 * mvn test -pl presto-hive \
 *   -Dtest=BenchmarkPushProjectionThroughCrossJoin \
 *   -DfailIfNoTests=false
 * </pre>
 */
public final class BenchmarkPushProjectionThroughCrossJoin
{
    private static final String QUERY =
            "SELECT " +
            "  regexp_replace(l.comment, '[aeiou]', '*') AS l_redacted, " +
            "  regexp_extract(l.comment, '\\w+') AS l_first_word, " +
            "  upper(reverse(l.shipinstruct)) AS l_instruct, " +
            "  regexp_replace(n.comment, '[aeiou]', '*') AS n_redacted, " +
            "  upper(reverse(n.name)) AS n_reversed, " +
            "  length(l.comment) + n.nationkey AS mixed " +
            "FROM lineitem l " +
            "CROSS JOIN nation n";

    @Test
    public void benchmark()
            throws Exception
    {
        try (HiveDistributedBenchmarkRunner runner =
                     new HiveDistributedBenchmarkRunner(3, 5)) {
            runner.addScenario("baseline", builder -> {
                builder.setSystemProperty("push_projection_through_cross_join", "false");
            });

            runner.addScenario("push_projection_through_cross_join", builder -> {
                builder.setSystemProperty("push_projection_through_cross_join", "true");
            });

            runner.runWithVerification(QUERY);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        new BenchmarkPushProjectionThroughCrossJoin().benchmark();
    }
}
