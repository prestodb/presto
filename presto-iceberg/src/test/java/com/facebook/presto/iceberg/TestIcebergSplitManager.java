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
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.TableProperties;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveCommonSessionProperties.AFFINITY_SCHEDULING_FILE_SECTION_SIZE;
import static com.facebook.presto.hive.HiveCommonSessionProperties.NODE_SELECTION_STRATEGY;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.TARGET_SPLIT_SIZE_BYTES;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestIcebergSplitManager
        extends AbstractTestQueryFramework
{
    protected TestIcebergSplitManager() {}

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build().getQueryRunner();
    }

    @Test
    public void testGetSplitsByPartitionMixNormalColumnsWithFilterPushdown()
    {
        testGetSplitsByPartitionMixNormalColumns("get_splits_with_filter_pushdown", true);
    }

    @Test
    public void testGetSplitsByPartitionMixNormalColumnsWithoutFilterPushdown()
    {
        testGetSplitsByPartitionMixNormalColumns("get_splits_without_filter_pushdown", false);
    }

    @Test
    public void testGetSplitsByNonIdentityPartitionColumnsWithFilterPushdown()
    {
        testGetSplitsByNonIdentityPartitionColumns("get_splits_by_nonidentity_with_filter_pushdown", true);
    }

    @Test
    public void testGetSplitsByNonIdentityPartitionColumnsWithoutFilterPushdown()
    {
        testGetSplitsByNonIdentityPartitionColumns("get_splits_by_nonidentity_without_filter_pushdown", false);
    }

    private void testGetSplitsByPartitionMixNormalColumns(String tableName, boolean filterPushdown)
    {
        assertUpdate("CREATE TABLE " + tableName + " (a varchar, b integer, r row(c int, d varchar)) WITH(partitioning = ARRAY['a'])");

        // Firstly, we build an Iceberg table which has three files:
        //      file1: partition column a's value is 'var1', and column b's value between (1, 3)
        //      file2: partition column a's value is 'var2', and column b's value between (8, 10)
        //      flie3: partition column a's value is 'var1', and column b's value between (2, 9)
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 1, (1001, 't1')), ('var1', 3, (1003, 't3'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var2', 8, (1008, 't8')), ('var2', 10, (1010, 't10'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 2, (1002, 't2')), ('var1', 9, (1009, 't9'))", 2);

        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        SplitManager splitManager = getQueryRunner().getSplitManager();

        // Get a plan for query on <tableName> with filter `a = 'var1'`
        // There should be 2 splits after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = 'var1'",
                2);

        // Get a plan for query on <tableName> with filter `a = 'var1' and b = 5`
        // There should leave only 1 split after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = 'var1' and b = 5",
                1);

        // Get a plan for query on <tableName> with filter `b = 8`
        // There should be 2 splits after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where b = 8",
                2);

        // Get a plan for query on <tableName> with filter `b = 6`
        // There should leave only 1 splits after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where b = 6",
                1);

        // Get a plan for query on <tableName> with filter `a = 'var1' and r.c = 1008`
        // There should be 2 splits after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = 'var1' and r.c = 1008",
                2);

        assertQuerySucceeds("DROP TABLE " + tableName);
    }

    private void testGetSplitsByNonIdentityPartitionColumns(String tableName, boolean filterPushdown)
    {
        // Take transformer 'year()' for example
        assertUpdate("CREATE TABLE " + tableName + " (a date, b integer, r row(c int, d varchar)) WITH(partitioning = ARRAY['year(a)'])");

        // Firstly, we build an Iceberg table which has three files:
        //      file1: partition column a's value is year(1984) with range ('1984-01-08', '1984-07-08'), and column b's value between (1, 3)
        //      file2: partition column a's value is year(2001) with range ('2001-01-01', '2001-11-01'), and column b's value between (8, 10)
        //      flie3: partition column a's value is year(1984) with range ('1984-06-08', '1984-12-08'), and column b's value between (2, 9)
        assertUpdate("INSERT INTO " + tableName + " VALUES (date '1984-01-08', 1, (1001, 't1')), (date '1984-07-08', 3, (1003, 't3'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (date '2001-01-01', 8, (1008, 't8')), (date '2001-11-01', 10, (1010, 't10'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES (date '1984-06-08', 2, (1002, 't2')), (date '1984-12-08', 9, (1009, 't9'))", 2);

        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        SplitManager splitManager = getQueryRunner().getSplitManager();

        // Get a plan for query on <tableName> with filter `a = date '1984-03-01'`
        // There should leave only 1 split after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = date '1984-03-01'",
                1);

        // Get a plan for query on <tableName> with filter `a = date '1984-06-09'`
        // There should be 2 splits after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = date '1984-06-09'",
                2);

        // Get a plan for query on <tableName> with filter `a = date '1984-06-09' and b = 8`
        // There should leave only 1 split after file scanning with the filter
        validateSplitsPlannedForSql(splitManager, transactionManager, filterPushdown,
                "select * from " + tableName + " where a = date '1984-06-09' and b = 8",
                1);

        assertQuerySucceeds("DROP TABLE " + tableName);
    }

    @Test
    public void testSplitSchedulingWithTablePropertyAndSession()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", IcebergSessionProperties.TARGET_SPLIT_SIZE_BYTES, "0")
                .build();
        assertQuerySucceeds("CREATE TABLE test_split_size as SELECT * FROM UNNEST(sequence(1, 512)) as t(i)");
        // verify that the session property hasn't propagated into the table
        assertEquals(getQueryRunner().execute("SELECT value FROM \"test_split_size$properties\" WHERE key = 'read.split.target-size'").getOnlyValue(),
                Long.toString(TableProperties.SPLIT_SIZE_DEFAULT));
        assertQuerySucceeds("ALTER TABLE test_split_size SET PROPERTIES (\"read.split.target-size\" = 1)");
        String selectQuery = "SELECT * FROM test_split_size";
        long maxSplits = getSplitsForSql(session, selectQuery).size();

        IntStream.range(1, 5)
                .mapToObj(i -> Math.pow(2, i))
                .forEach(splitSize -> {
                    assertQuerySucceeds("ALTER TABLE test_split_size SET PROPERTIES (\"read.split.target-size\" =" + splitSize.intValue() + ")");
                    assertEquals(getSplitsForSql(session, selectQuery).size(), (double) maxSplits / splitSize, 5);
                });
        // split size should be set to 32 on the table property.
        // Set it to 1 with the session property to override the table value and verify we get the
        // same number of splits as when the table value is set to 1.
        Session minSplitSession = Session.builder(session)
                .setCatalogSessionProperty("iceberg", TARGET_SPLIT_SIZE_BYTES, "1")
                .build();
        assertEquals(getSplitsForSql(minSplitSession, selectQuery).size(), maxSplits);
        assertQuerySucceeds("DROP TABLE test_split_size");
    }

    @Test
    public void testSoftAffinitySchedulingSectionConfig()
    {
        Session maxIdentifiers = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", AFFINITY_SCHEDULING_FILE_SECTION_SIZE, "1B")
                .setCatalogSessionProperty("iceberg", TARGET_SPLIT_SIZE_BYTES, "1")
                .setCatalogSessionProperty("iceberg", NODE_SELECTION_STRATEGY, "SOFT_AFFINITY")
                .build();
        assertQuerySucceeds("CREATE TABLE test_affinity_section_scheduling as SELECT * FROM UNNEST(sequence(1, 512)) as t(i)");
        String selectQuery = "SELECT * FROM test_affinity_section_scheduling";
        Function<Session, Set<String>> getIdentifiers = (session) -> {
            List<Split> splits = getSplitsForSql(session, selectQuery);
            Set<String> allIdentifiers = new HashSet<>();
            splits.stream().map(Split::getConnectorSplit)
                    .forEach(connectorSplit -> connectorSplit.getPreferredNodes(identifier -> {
                        allIdentifiers.add(identifier);
                        return ImmutableList.of();
                    }));
            return allIdentifiers;
        };
        Set<String> maxSplitsIds = getIdentifiers.apply(maxIdentifiers);
        Set<String> halfSplitIds = getIdentifiers.apply(Session.builder(maxIdentifiers)
                .setCatalogSessionProperty("iceberg", AFFINITY_SCHEDULING_FILE_SECTION_SIZE, "2B").build());
        assertEquals((double) halfSplitIds.size() / maxSplitsIds.size(), 0.5, 1E-10);

        Set<String> singleSplitId = getIdentifiers.apply(Session.builder(maxIdentifiers)
                .setCatalogSessionProperty("iceberg", AFFINITY_SCHEDULING_FILE_SECTION_SIZE, "1GB").build());
        assertEquals(singleSplitId.size(), 1);
        assertQuerySucceeds("DROP TABLE test_affinity_section_scheduling");
    }

    private Session sessionWithFilterPushdown(boolean pushdown)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, pushdown ? "true" : "false")
                .build();
    }

    private List<Split> getSplitsForSql(Session session, String sql)
    {
        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        SplitManager splitManager = getQueryRunner().getSplitManager();

        List<TableScanNode> tableScanNodes = getTableScanFromOptimizedPlanOfSql(sql, session);
        assertNotNull(tableScanNodes);
        assertEquals(tableScanNodes.size(), 1);

        TransactionId transactionId = transactionManager.beginTransaction(false);
        session = session.beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        TableHandle tableHandle = tableScanNodes.get(0).getTable();
        TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(),
                tableHandle.getConnectorHandle(),
                transactionManager.getConnectorTransaction(transactionId, tableHandle.getConnectorId()),
                tableHandle.getLayout(),
                tableHandle.getDynamicFilter());

        try (SplitSource splitSource = splitManager.getSplits(session, newTableHandle, SplitSchedulingStrategy.UNGROUPED_SCHEDULING, WarningCollector.NOOP)) {
            ImmutableList.Builder<Split> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1024).get().getSplits());
            }
            assertTrue(splitSource.isFinished());
            return splits.build();
        }
        catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            transactionManager.asyncAbort(transactionId);
        }
    }

    private void validateSplitsPlannedForSql(SplitManager splitManager,
            TransactionManager transactionManager,
            boolean filterPushdown,
            String sql,
            int expectedSplitCount)
    {
        Session session = sessionWithFilterPushdown(filterPushdown);
        List<TableScanNode> tableScanNodes = getTableScanFromOptimizedPlanOfSql(sql, session);
        assertNotNull(tableScanNodes);
        assertEquals(tableScanNodes.size(), 1);

        TransactionId transactionId = transactionManager.beginTransaction(false);
        session = session.beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());
        TableHandle tableHandle = tableScanNodes.get(0).getTable();
        TableHandle newTableHandle = new TableHandle(tableHandle.getConnectorId(),
                tableHandle.getConnectorHandle(),
                transactionManager.getConnectorTransaction(transactionId, tableHandle.getConnectorId()),
                tableHandle.getLayout(),
                tableHandle.getDynamicFilter());

        try (SplitSource splitSource = splitManager.getSplits(session, newTableHandle, SplitSchedulingStrategy.UNGROUPED_SCHEDULING, WarningCollector.NOOP)) {
            SplitBatch splitBatch = splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1024).get();
            assertTrue(splitSource.isFinished());
            assertEquals(splitBatch.getSplits().size(), expectedSplitCount);
        }
        catch (Exception e) {
            fail("Should not throw exception when getting split source: " + e.getMessage());
        }
        finally {
            transactionManager.asyncAbort(transactionId);
        }
    }

    private List<TableScanNode> getTableScanFromOptimizedPlanOfSql(String sql, Session session)
    {
        PlanNode planNode = plan(sql, session).getRoot();

        TableScanNodesExtractingVisitor insertValuesExtractingVisitor = new TableScanNodesExtractingVisitor();
        insertValuesExtractingVisitor.visitPlan(planNode, null);
        return insertValuesExtractingVisitor.getTableScanNodes();
    }

    private static class TableScanNodesExtractingVisitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final List<TableScanNode> tableScanNodes = new ArrayList<>();

        public List<TableScanNode> getTableScanNodes()
        {
            return tableScanNodes;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            if (node.getSources() != null) {
                for (PlanNode source : node.getSources()) {
                    source.accept(this, context);
                }
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            tableScanNodes.add(node);
            return null;
        }
    }
}
