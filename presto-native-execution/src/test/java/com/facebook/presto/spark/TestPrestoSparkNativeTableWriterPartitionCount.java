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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_TABLE_WRITER_PARTITION_COUNT;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkNativeTableWriterPartitionCount
        extends AbstractTestQueryFramework
{
    // Native writers name the files of a table that is not bucketed {taskId}_{driverId}_{planNodeId}_{uuid}, and a task id
    // ends with .{stageId}.{stageExecutionId}.{partitionId}.{attemptNumber}
    private static final Pattern WRITER_PARTITION_ID = Pattern.compile("/\\d{8}_\\d{6}_\\d{5}_[a-z0-9]+\\.\\d+\\.\\d+\\.(\\d+)\\.\\d+_[^/]+$");

    @Override
    protected void createTables()
    {
        createOrders((QueryRunner) getExpectedQueryRunner());
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        return PrestoSparkNativeQueryRunnerUtils.createHiveRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testPartitionedTable()
    {
        assertPartitionedTableWrite(
                Session.builder(getSession())
                        .setSystemProperty(SPARK_TABLE_WRITER_PARTITION_COUNT, "4")
                        .build(),
                "table_writer_partition_count_partitioned");
    }

    @Test
    public void testPartitionedTableWithAdaptiveExecution()
    {
        assertPartitionedTableWrite(
                Session.builder(getSession())
                        .setSystemProperty(SPARK_TABLE_WRITER_PARTITION_COUNT, "4")
                        .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, "true")
                        .build(),
                "table_writer_partition_count_adaptive");
    }

    @Test
    public void testBucketedTable()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SPARK_TABLE_WRITER_PARTITION_COUNT, "2")
                .build();
        try {
            assertUpdate(
                    session,
                    "CREATE TABLE table_writer_partition_count_bucketed WITH (bucketed_by = ARRAY['orderkey'], bucket_count = 13) AS " +
                            "SELECT orderkey, custkey, orderstatus FROM orders",
                    15000);
            assertQuery(
                    "SELECT CAST(\"$bucket\" AS BIGINT), count(*) FROM table_writer_partition_count_bucketed GROUP BY \"$bucket\"",
                    "SELECT orderkey % 13, count(*) FROM orders GROUP BY orderkey % 13");
            // a bucket is never split between writers
            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM table_writer_partition_count_bucketed").getOnlyValue(), 13L);
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS table_writer_partition_count_bucketed");
        }
    }

    private void assertPartitionedTableWrite(Session session, String table)
    {
        try {
            // The aggregation spreads the rows of every table partition over all of its hash partitioned
            // tasks, so without the shuffle on the partition columns each task writes a file per partition
            assertUpdate(
                    session,
                    format("CREATE TABLE %s WITH (partitioned_by = ARRAY['orderpriority', 'orderstatus']) AS ", table) +
                            "SELECT orderkey, custkey, orderpriority, orderstatus FROM orders GROUP BY orderkey, custkey, orderpriority, orderstatus",
                    15000);
            assertQuery(
                    format("SELECT orderkey, custkey, orderpriority, orderstatus FROM %s", table),
                    "SELECT orderkey, custkey, orderpriority, orderstatus FROM orders");
            // a single writer per table partition writes one file for each of the 15 partitions
            assertEquals(computeActual(format("SELECT count(DISTINCT \"$path\") FROM %s", table)).getOnlyValue(), 15L);
            // the 15 partitions are spread over all 4 writer tasks
            assertEquals(getWriterPartitionIds(table), ImmutableSet.of(0, 1, 2, 3));
        }
        finally {
            assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    private Set<Integer> getWriterPartitionIds(String table)
    {
        return computeActual(format("SELECT DISTINCT \"$path\" FROM %s", table)).getOnlyColumn()
                .map(path -> {
                    Matcher matcher = WRITER_PARTITION_ID.matcher((String) path);
                    assertTrue(matcher.find(), "unexpected file name: " + path);
                    return Integer.parseInt(matcher.group(1));
                })
                .collect(toImmutableSet());
    }
}
