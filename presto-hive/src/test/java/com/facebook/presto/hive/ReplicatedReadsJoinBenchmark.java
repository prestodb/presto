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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.AbstractSqlBenchmark;
import com.facebook.presto.benchmark.SimpleLineBenchmarkResultWriter;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.hive.HiveBenchmarkQueryRunner.createLocalQueryRunner;

public class ReplicatedReadsJoinBenchmark
        extends AbstractSqlBenchmark
{
    private static final Logger LOGGER = Logger.get(ReplicatedReadsJoinBenchmark.class);
    private static final String LOGMSG_NO_RR = "Without replicated-reads";
    private static final String LOGMSG_RR = "With replicated-reads";
    private static LocalQueryRunner localQueryRunner;
    private static LocalQueryRunner localQueryRunnerReplicatedReadsEnabled;

    protected ReplicatedReadsJoinBenchmark(LocalQueryRunner localQueryRunner, String benchmarkName, int warmupIterations, int measuredIterations, String query)
    {
        super(localQueryRunner, benchmarkName, warmupIterations, measuredIterations, query);
    }

    public static void main(String[] args)
    {
        File tempDir = Files.createTempDir();
        Map<String, String> systemProperties = ImmutableMap.<String, String>builder()
                .put("join_distribution_type", "broadcast")
                .build();
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.replicated-reads-for-broadcast-join-enabled", "true")
                .build();
        localQueryRunner = createLocalQueryRunner(tempDir, systemProperties, ImmutableMap.of());
        localQueryRunnerReplicatedReadsEnabled = createLocalQueryRunner(tempDir, systemProperties, hiveProperties);
        startBenchmark();
    }
    private static void startBenchmark()
    {
        // start benchmark
        benchmarkTwoWayReplicatedReads();
        benchmarkThressWayReplicatedReads();
        benchmarkInSubqueryForSemijoinReplicatedReads();
    }

    private static void benchmarkTwoWayReplicatedReads()
    {
        String benchmarkName = "2_way_join_for_replicated_reads_benchmark_test";
        String query = "select 1 from orders o join lineitem l on o.orderkey=l.orderkey";

        execute(benchmarkName, query);
    }

    private static void benchmarkThressWayReplicatedReads()
    {
        String benchmarkName = "2_way_join_for_replicated_reads_benchmark_test";
        String query = "select 1 from customer c, nation n, region r where c.nationkey=n.nationkey and n.regionkey=r.regionkey";

        execute(benchmarkName, query);
    }

    private static void benchmarkInSubqueryForSemijoinReplicatedReads()
    {
        String benchmarkName = "InSubquery test";
        String query = "SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderkey = custkey)";

        execute(benchmarkName, query);
    }

    private static void execute(String benchmarkName, String query)
    {
        execute(localQueryRunner, benchmarkName, query, LOGMSG_NO_RR);
        execute(localQueryRunnerReplicatedReadsEnabled, benchmarkName, query, LOGMSG_RR);
    }

    private static void execute(LocalQueryRunner localQueryRunner, String benchmarkName, String query, String logmsg)
    {
        LOGGER.info(logmsg);
        new ReplicatedReadsJoinBenchmark(localQueryRunner, benchmarkName, 1, 3, query)
                .runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
