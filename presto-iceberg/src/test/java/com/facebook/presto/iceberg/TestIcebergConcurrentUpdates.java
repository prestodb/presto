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
 * Verifies how Presto handles concurrent row-level modifications ({@code UPDATE} and {@code MERGE}) on Iceberg tables,
 * using {@link TestingGateFunctions} to deterministically orchestrate snapshot commit conflicts.
 * <p>
 * <b>Iceberg Concurrent Update Semantics:</b>
 * An {@code UPDATE} of an Iceberg table is a merge-on-read operation: it writes position delete files for
 * the rows it changes together with their new versions, and stages both as a row delta on top of the
 * snapshot the statement reads. Committing that row delta re-applies it to the latest table metadata and
 * checks that no delete file has appeared since that snapshot, because another writer may have deleted rows
 * this statement is rewriting. The check spans the whole table rather than the rows the statement touches,
 * so two updates running at the same time conflict even when they change different rows: the first one
 * commits a new snapshot, and the second one finds those delete files and has its commit rejected with an
 * Iceberg {@code org.apache.iceberg.exceptions.ValidationException}. The loser of the race writes nothing,
 * so the engine is expected to run the statement again against the new state of the table rather than fail it.
 * <p>
 * <b>Role of {@link TestingGateFunctions}:</b>
 * Relying on thread scheduling alone cannot guarantee that two statements read the exact same table snapshot
 * before either commits. This test installs {@link TestingGateFunctions.TestingGatePlugin} and appends
 * {@code AND await_gate('<gate_name>')} to background {@code UPDATE} statements so that:
 * <ol>
 *   <li>Each background {@code UPDATE} starts a transaction, pins the current Iceberg snapshot, begins scanning rows,
 *       and blocks inside {@code await_gate} upon evaluating the matching row.</li>
 *   <li>The main test thread calls {@link TestingGateFunctions.Gate#awaitArrival} to confirm all background writers
 *       have pinned the initial snapshot and are paused mid-execution.</li>
 *   <li>While the background writers are held at their gates, the main thread executes an ungated {@code UPDATE}
 *       or {@code MERGE} that reads the same snapshot and commits first, producing new delete files and advancing
 *       the table snapshot.</li>
 *   <li>The main thread releases the gates via {@link TestingGateFunctions.Gate#release()}, allowing the paused
 *       background writers to finish execution and attempt to commit against the now-stale snapshot, which deterministically
 *       triggers an {@code ICEBERG_COMMIT_CONFLICT} and exercises Presto's automatic query retry mechanism.</li>
 *   <li>When Presto retries the failed queries, their gates are already in the released state, so {@code await_gate}
 *       returns {@code true} immediately without blocking again.</li>
 * </ol>
 */
public class TestIcebergConcurrentUpdates
        extends AbstractTestQueryFramework
{
    private static final int WRITERS = 4; // Total number of different UPDATE statements executed over the same table.
    private static final int ROUNDS = 5;  // Total number of rounds of concurrent updates.

    private ExecutorService executor;

    /**
     * Creates a distributed Iceberg query runner with {@link TestingGateFunctions.TestingGatePlugin} installed
     * so that tests can invoke the {@code await_gate(VARCHAR)} SQL function to coordinate concurrent queries.
     */
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
        queryRunner.installPlugin(new TestingGateFunctions.TestingGatePlugin());
        return queryRunner;
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

    /**
     * Verifies that when multiple concurrent {@code UPDATE} statements read the same Iceberg table snapshot,
     * the statements that lose the commit race are automatically retried by Presto until all updates succeed.
     * <p>
     * In each round, {@code WRITERS - 1} background {@code UPDATE} queries are paused mid-execution using
     * {@link TestingGateFunctions} after pinning the current snapshot, while one ungated {@code UPDATE} commits
     * a new snapshot. Releasing the gates forces every gated {@code UPDATE} to encounter a commit conflict and retry.
     */
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
            List<TestingGateFunctions.Gate> gates = new ArrayList<>();
            List<String> gateNames = new ArrayList<>();
            List<Future<?>> updates = new ArrayList<>();
            for (int writer = 1; writer < WRITERS; writer++) {
                String gateName = format("concurrent_update_%s_%s", round, writer);
                gateNames.add(gateName);
                gates.add(TestingGateFunctions.createGate(gateName));
                String sql = format("UPDATE %s SET value = value + 1 WHERE id = %s AND await_gate('%s')", tableName, writer, gateName);
                updates.add(executor.submit(() -> getQueryRunner().execute(session, sql)));
            }

            try {
                try {
                    for (TestingGateFunctions.Gate gate : gates) {
                        gate.awaitArrival(30, SECONDS);
                    }

                    // This writer commits after the other updates have pinned their snapshots.
                    getQueryRunner().execute(session, format("UPDATE %s SET value = value + 1 WHERE id = %s", tableName, WRITERS));
                }
                finally {
                    gates.forEach(TestingGateFunctions.Gate::release);
                }
                for (Future<?> update : updates) {
                    // throws ICEBERG_COMMIT_CONFLICT if a rejected commit is not retried
                    update.get();
                }
            }
            finally {
                gates.forEach(TestingGateFunctions.Gate::release);
                gateNames.forEach(TestingGateFunctions::removeGate);
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
        String sql = format("UPDATE %s SET value = value \\+ 1 WHERE id = .* AND await_gate\\('.*'\\)", tableName);
        assertTrue(countRetriedQueries(sql) >= (WRITERS - 1) * ROUNDS,
                "expected every gated update to be retried at least once");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * Verifies that an {@code UPDATE} statement conflicting with a concurrent {@code MERGE} statement over the same
     * Iceberg snapshot is automatically retried and both statements' changes are preserved.
     * <p>
     * Uses {@link TestingGateFunctions} to hold the {@code UPDATE} in-flight after it pins the initial table snapshot,
     * executes the {@code MERGE} to completion so it commits a new snapshot with delete files, and then releases the gate
     * so the {@code UPDATE} hits a commit conflict and retries against the post-{@code MERGE} snapshot.
     */
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

        // Execute an UPDATE and MERGE over the same snapshot. The MERGE commits while the UPDATE is gated.
        String gateName = "update_before_merge";
        TestingGateFunctions.Gate gate = TestingGateFunctions.createGate(gateName);
        String updateSql = format("UPDATE %s SET value = value + 1 WHERE id = 1 AND await_gate('%s')", tableName, gateName);
        Future<?> update = executor.submit(() -> getQueryRunner().execute(session, updateSql));

        // MERGE modifies row with id = 2 and inserts row with id = 3
        String mergeSql = format(
                "MERGE INTO %s t USING (SELECT 2 AS id, 100 AS value UNION ALL SELECT 3 AS id, 300 AS value ) s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN " +
                "  UPDATE SET value = s.value " +
                "WHEN NOT MATCHED THEN" +
                "  INSERT (id, value) VALUES(s.id, s.value)",
                tableName);

        try {
            gate.awaitArrival(30, SECONDS);
            getQueryRunner().execute(session, mergeSql);
        }
        finally {
            gate.release();
        }

        try {
            // throws ICEBERG_COMMIT_CONFLICT if the rejected UPDATE is not retried
            update.get();
        }
        finally {
            TestingGateFunctions.removeGate(gateName);
        }

        // After executing both UPDATE and MERGE commands, the table must contain the following data:
        // 1, 11 (updated by UPDATE)
        // 2, 100 (updated by MERGE)
        // 3, 300 (inserted by MERGE)

        // Check the "test_concurrent_update_merge" table contains the expected data.
        assertQuery(session, format("SELECT id, value FROM %s ORDER BY id", tableName),
                "VALUES (1, 11), (2, 100), (3, 300)");

        // Check that the gated UPDATE was retried after its commit conflict.
        String updatePattern = format("UPDATE %s SET value = value \\+ 1 WHERE id = 1 AND await_gate\\('%s'\\)", tableName, gateName);
        String mergePattern = format("MERGE INTO %s t USING .* ON t.id = s.id .*", tableName);
        long retriedUpdates = countRetriedQueries(updatePattern);
        long retriedMerges = countRetriedQueries(mergePattern);
        assertEquals(retriedUpdates, 1L, "expected the gated UPDATE to be retried once");
        assertEquals(retriedMerges, 0L, "expected the MERGE to commit without a retry");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * Counts how many queries recorded by the coordinator's {@code QueryManager} were retry attempts
     * matching the given SQL regular expression pattern.
     */
    private long countRetriedQueries(String queryPattern)
    {
        Pattern retryPattern = Pattern.compile("-- retry query.*?; attempt:.*?" + queryPattern, Pattern.DOTALL);
        return ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> retryPattern.matcher(query.getQuery()).matches())
                .count();
    }

    /**
     * Verifies that when an {@code UPDATE} runs inside an explicit multi-statement transaction ({@code START TRANSACTION} ...
     * {@code COMMIT}) and conflicts with another concurrent {@code UPDATE}, the conflict is not silently retried and is
     * surfaced to the client at {@code COMMIT} time.
     */
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
