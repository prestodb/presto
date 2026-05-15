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

import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeMetric;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED;
import static com.facebook.presto.SystemSessionProperties.QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED;
import static com.facebook.presto.common.RuntimeMetricName.OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT;
import static com.facebook.presto.hive.HiveMetadata.REFERENCED_MATERIALIZED_VIEWS;
import static com.facebook.presto.hive.TestHiveLogicalPlanner.replicateHiveMetastore;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveMaterializedViewRewriteRuntimeMetric
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS),
                ImmutableMap.of("experimental.allow-legacy-materialized-views-toggle", "true"),
                Optional.empty());
    }

    @Test
    public void testMaterializedViewRewriteRuntimeMetric()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_metric_test";
        String view = "orders_view_metric_test";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, '2020-01-02' as ds FROM orders WHERE orderkey > 1000", table));
            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT orderkey, orderpriority, ds FROM %s", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);
            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            // ds='2020-01-01' partition is refreshed, so the optimizer should substitute the MV
            // and the OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT metric must increment.
            String query = format("SELECT orderkey, orderpriority FROM %s WHERE ds='2020-01-01' ORDER BY orderkey", table);
            assertMaterializedViewRewriteOccurred(session, query);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testMaterializedViewRewriteRuntimeMetricNotFiredForUnsupportedFunction()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUERY_OPTIMIZATION_WITH_MATERIALIZED_VIEW_ENABLED, "true")
                .setSystemProperty(MATERIALIZED_VIEW_DATA_CONSISTENCY_ENABLED, "false")
                .build();
        QueryRunner queryRunner = getQueryRunner();
        String table = "orders_partitioned_neg_metric_test";
        String view = "orders_view_neg_metric_test";
        try {
            queryRunner.execute(format("CREATE TABLE %s WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, custkey, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, custkey, '2020-01-02' as ds FROM orders WHERE orderkey > 1000", table));
            assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioned_by = ARRAY['ds']) " +
                    "AS SELECT max(custkey) as max_custkey, orderkey, ds FROM %s GROUP BY ds, orderkey", view, table));
            assertTrue(getQueryRunner().tableExists(getSession(), view));
            assertUpdate(format("REFRESH MATERIALIZED VIEW %s WHERE ds='2020-01-01'", view), 255);

            setReferencedMaterializedViews((DistributedQueryRunner) queryRunner, table, ImmutableList.of(view));

            String query = format("SELECT GEOMETRIC_MEAN(custkey), orderkey FROM %s GROUP BY orderkey ORDER BY orderkey", table);
            assertMaterializedViewRewriteDidNotOccur(session, query);
        }
        finally {
            queryRunner.execute("DROP MATERIALIZED VIEW IF EXISTS " + view);
            queryRunner.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    /**
     * Runs the given query and asserts that the materialized-view rewrite runtime metric was incremented
     * (i.e. the optimizer rewrote at least one query specification to read from a materialized view).
     * Returns a result coerced via toTestTypes() so it is interchangeable with computeActual(...).
     */
    private MaterializedResult assertMaterializedViewRewriteOccurred(Session session, String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(session, sql);
        QueryId queryId = result.getQueryId();
        QueryInfo queryInfo = runner.getQueryInfo(queryId);
        RuntimeStats runtimeStats = queryInfo.getQueryStats().getRuntimeStats();
        RuntimeMetric metric = runtimeStats.getMetric(OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT);
        assertNotNull(metric, format("Expected runtime metric %s to be present for query %s", OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, queryId));
        assertTrue(metric.getSum() > 0, format("Expected %s > 0, was %d", OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, metric.getSum()));
        return result.getResult().toTestTypes();
    }

    /**
     * Runs the given query and asserts that the materialized-view rewrite runtime metric was NOT incremented
     * (i.e. the optimizer did not rewrite any query specification to read from a materialized view).
     * Returns a result coerced via toTestTypes() so it is interchangeable with computeActual(...).
     */
    private MaterializedResult assertMaterializedViewRewriteDidNotOccur(Session session, String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(session, sql);
        QueryInfo queryInfo = runner.getQueryInfo(result.getQueryId());
        RuntimeStats runtimeStats = queryInfo.getQueryStats().getRuntimeStats();
        RuntimeMetric metric = runtimeStats.getMetric(OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT);
        long actual = metric == null ? 0 : metric.getSum();
        assertTrue(actual == 0, format("Expected %s == 0, was %d", OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT, actual));
        return result.getResult().toTestTypes();
    }

    private void setReferencedMaterializedViews(DistributedQueryRunner queryRunner, String tableName, List<String> referencedMaterializedViews)
    {
        appendTableParameter(replicateHiveMetastore(queryRunner),
                tableName,
                REFERENCED_MATERIALIZED_VIEWS,
                referencedMaterializedViews.stream().map(view -> format("%s.%s", getSession().getSchema().orElse(""), view)).collect(joining(",")));
    }

    private void appendTableParameter(ExtendedHiveMetastore metastore, String tableName, String parameterKey, String parameterValue)
    {
        MetastoreContext metastoreContext = new MetastoreContext(getSession().getUser(), getSession().getQueryId().getId(), Optional.empty(), Collections.emptySet(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, getSession().getWarningCollector(), getSession().getRuntimeStats());
        Optional<Table> table = metastore.getTable(metastoreContext, getSession().getSchema().get(), tableName);
        if (table.isPresent()) {
            Table originalTable = table.get();
            Table alteredTable = Table.builder(originalTable).setParameter(parameterKey, parameterValue).build();
            metastore.dropTable(metastoreContext, originalTable.getDatabaseName(), originalTable.getTableName(), false);
            metastore.createTable(metastoreContext, alteredTable, new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()), emptyList());
        }
    }
}
