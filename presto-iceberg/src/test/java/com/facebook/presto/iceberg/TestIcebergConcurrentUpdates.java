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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.facebook.presto.SystemSessionProperties.QUERY_RETRY_LIMIT;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * An UPDATE of an Iceberg table is a merge-on-read operation: it writes position delete files for
 * the rows it changes together with their new versions, and stages both as a row delta on top of the
 * snapshot the statement reads. Committing that row delta re-applies it to the latest table metadata and
 * checks that no delete file has appeared since that snapshot, because another writer may have deleted rows
 * this statement is rewriting. The check spans the whole table rather than the rows the statement touches,
 * so two updates running at the same time conflict even when they change different rows: the first one
 * commits a new snapshot, and the second one finds those delete files and has its commit rejected with an
 * Iceberg {@code org.apache.iceberg.exceptions.ValidationException}. The loser of the race writes nothing,
 * so the engine is expected to run the statement again against the new state of the table rather than fail it.
 */
public class TestIcebergConcurrentUpdates
        extends AbstractTestQueryFramework
{
    private static final int WRITERS = 4; // Total number of different UPDATE statements executed over the same table.
    private static final int ROUNDS = 5;  // Total number of rounds of concurrent updates.

    private ExecutorService executor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    @BeforeClass(alwaysRun = true)
    public void setUp()
    {
        executor = newFixedThreadPool(WRITERS);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testConcurrentUpdatesAreRetried()
            throws Exception
    {
        String tableName = "test_concurrent_updates";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(format("CREATE TABLE %s (id integer, value integer)", tableName));
        assertUpdate(format("INSERT INTO %s SELECT CAST(id AS integer), 0 FROM UNNEST(sequence(1, %s)) AS t(id)", tableName, WRITERS), WRITERS);

        // The "test_concurrent_updates" table contains the following data:
        // 1, 0
        // 2, 0
        // 3, 0
        // ...

        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_RETRY_LIMIT, String.valueOf(WRITERS * ROUNDS))
                .build();

        // Execute concurrent UPDATEs over the "test_concurrent_updates" table.
        for (int round = 0; round < ROUNDS; round++) {
            // each writer only touches its own row, so the statements only conflict when committing
            CyclicBarrier startTogether = new CyclicBarrier(WRITERS);
            List<Future<?>> updates = new ArrayList<>();
            for (int writer = 1; writer <= WRITERS; writer++) {
                String sql = format("UPDATE %s SET value = value + 1 WHERE id = %s", tableName, writer);
                updates.add(executor.submit(() -> {
                    startTogether.await(30, SECONDS);
                    return getQueryRunner().execute(session, sql);
                }));
            }

            for (Future<?> update : updates) {
                // throws ICEBERG_COMMIT_CONFLICT if a rejected commit is not retried
                update.get();
            }
        }

        // After executing all UPDATE commands, the table must contain the following data:
        // 1, 5
        // 2, 5
        // 3, 5
        // ...

        // Check the "test_concurrent_updates" table contains the expected data.
        // It means concurrent UPDATE commands that failed were retried and succeeded,
        // so the value of the "value" column for all rows is equal to the number of rounds.
        assertEquals(computeScalar(format("SELECT count(*) FROM %s WHERE value = %s", tableName, ROUNDS)), (long) WRITERS);

        // Check the list of executed queries and count the UPDATE commands that failed during the Iceberg commit and were re-executed.
        String sql = format("UPDATE %s SET value = value \\+ 1 WHERE id = .*", tableName);
        assertTrue(countRetriedQueries(sql) > 0, "expected concurrent updates to be retried");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testConcurrentUpdateAndMergeAreRetried()
            throws Exception
    {
        String tableName = "test_concurrent_update_merge";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(format("CREATE TABLE %s (id integer, value integer)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName), 2);

        // The "test_concurrent_update_merge" table contains the following data:
        // 1, 10
        // 2, 20

        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_RETRY_LIMIT, "10")
                .build();

        // Execute UPDATE and MERGE concurrently over the same table.
        // Both statements modify different rows but will conflict when committing to Iceberg.
        CyclicBarrier startTogether = new CyclicBarrier(2);
        List<Future<?>> operations = new ArrayList<>();

        // UPDATE modifies row with id = 1
        String updateSql = format("UPDATE %s SET value = value + 1 WHERE id = 1", tableName);
        operations.add(executor.submit(() -> {
            startTogether.await(30, SECONDS);
            return getQueryRunner().execute(session, updateSql);
        }));

        // MERGE modifies row with id = 2 and inserts row with id = 3
        String mergeSql = format(
                "MERGE INTO %s t USING (SELECT 2 AS id, 100 AS value UNION ALL SELECT 3 AS id, 300 AS value ) s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN " +
                "  UPDATE SET value = s.value " +
                "WHEN NOT MATCHED THEN" +
                "  INSERT (id, value) VALUES(s.id, s.value)",
                tableName);

        operations.add(executor.submit(() -> {
            startTogether.await(30, SECONDS);
            return getQueryRunner().execute(session, mergeSql);
        }));

        for (Future<?> operation : operations) {
            // throws ICEBERG_COMMIT_CONFLICT if a rejected commit is not retried
            operation.get();
        }

        // After executing both UPDATE and MERGE commands, the table must contain the following data:
        // 1, 11 (updated by UPDATE)
        // 2, 100 (updated by MERGE)
        // 3, 300 (inserted by MERGE)

        // Check the "test_concurrent_update_merge" table contains the expected data.
        assertQuery(session, format("SELECT id, value FROM %s ORDER BY id", tableName),
                "VALUES (1, 11), (2, 100), (3, 300)");

        // Check that at least one operation was retried due to commit conflict.
        String updatePattern = format("UPDATE %s SET value = value \\+ 1 WHERE id = 1", tableName);
        String mergePattern = format("MERGE INTO %s t USING .* ON t.id = s.id .*", tableName);
        long retriedUpdates = countRetriedQueries(updatePattern);
        long retriedMerges = countRetriedQueries(mergePattern);
        assertTrue(retriedUpdates > 0 || retriedMerges > 0,
                "expected at least one concurrent operation (UPDATE or MERGE) to be retried");

        assertUpdate("DROP TABLE " + tableName);
    }

    private long countRetriedQueries(String queryPattern)
    {
        Pattern retryPattern = Pattern.compile("-- retry query.*?; attempt:.*?" + queryPattern, Pattern.DOTALL);
        return ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> retryPattern.matcher(query.getQuery()).matches())
                .count();
    }

    @Test
    public void testConflictingUpdateIsReportedAsRetriableError()
    {
        String tableName = "test_update_commit_conflict";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(format("CREATE TABLE %s (id integer, value integer)", tableName));
        assertUpdate(format("INSERT INTO %s VALUES (1, 10), (2, 20)", tableName), 2);

        // An explicit transaction is never retried, so the conflict is surfaced to the client.
        Session session = getSession();
        Session transactionSession = assertStartTransaction(session, "START TRANSACTION");
        assertUpdate(transactionSession, format("UPDATE %s SET value = value + 1 WHERE id = 1", tableName), 1);

        // Execute an UPDATE command outside the transaction.
        // It commits while the transaction above still holds an uncommitted row delta for the same table.
        assertUpdate(session, format("UPDATE %s SET value = value + 100 WHERE id = 2", tableName), 1);

        // Commit the changes done by the UPDATE command executed in the transaction.
        assertQueryFails(transactionSession, "COMMIT",
                "Failed to commit changes to the Iceberg table tpch." + tableName + " because it was concurrently modified");

        // Check that only the UPDATE command outside the transaction has modified the table data.
        assertQuery(session, "SELECT id, value FROM " + tableName, "VALUES (1, 10), (2, 120)");
        assertUpdate(session, "DROP TABLE " + tableName);
    }
}
