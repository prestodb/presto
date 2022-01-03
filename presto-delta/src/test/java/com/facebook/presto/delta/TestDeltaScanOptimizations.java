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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.notNull;
import static com.facebook.presto.common.predicate.Domain.onlyNull;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.delta.DeltaSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

/**
 * Integrations tests for various optimization (such as filter pushdown, nested column project/filter pushdown etc)
 * that speed up reading data from Delta tables.
 */
public class TestDeltaScanOptimizations
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void filterOnRegularColumn()
    {
        String tableName = "data-reader-primitives";
        String testQuery = format("SELECT as_int, as_string FROM \"%s\" WHERE as_int = 1", tableName);
        String expResultsQuery = "SELECT 1, cast('1' as varchar)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of("as_int", singleValue(INTEGER, 1L)),
                Collections.emptyMap());
    }

    @Test
    public void filterOnPartitionColumn()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery = format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz')", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20180512', 'Jay', 'sh', 4),('20181212', 'Linda', 'sz', 8)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))),
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))));
    }

    @Test
    public void filterOnMultiplePartitionColumns()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery =
                format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz') AND \"date\" = '20180512'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20180512', 'Jay', 'sh', 4)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "date", singleValue(VARCHAR, utf8Slice("20180512"))),
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "date", singleValue(VARCHAR, utf8Slice("20180512"))));
    }

    @Test
    public void filterOnPartitionColumnAndRegularColumns()
    {
        String tableName = "deltatbl-partition-prune";
        String testQuery = format("SELECT date, name, city, cnt FROM \"%s\" WHERE city in ('sh', 'sz') AND name = 'Linda'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('20181212', 'Linda', 'sz', 8)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz"))),
                        "name", singleValue(VARCHAR, utf8Slice("Linda"))),
                ImmutableMap.of("city", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("sh"), utf8Slice("sz")))));
    }

    @Test
    public void nullPartitionFilter()
    {
        String tableName = "data-reader-partition-values";
        String testQuery =
                format("SELECT value, as_boolean FROM \"%s\" WHERE as_int is null and value is not null", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('2', null)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "as_int", onlyNull(INTEGER),
                        "value", notNull(VARCHAR)),
                ImmutableMap.of("as_int", onlyNull(INTEGER)));
    }

    @Test
    public void notNullPartitionFilter()
    {
        String tableName = "data-reader-partition-values";
        String testQuery = format("SELECT value, as_boolean FROM \"%s\" WHERE as_int is not null and value = '1'", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('1', false)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "as_int", notNull(INTEGER),
                        "value", singleValue(VARCHAR, utf8Slice("1"))),
                ImmutableMap.of("as_int", notNull(INTEGER)));
    }

    @Test
    public void nestedColumnFilter()
    {
        String tableName = "data-reader-nested-struct";
        String testQuery = format("SELECT a.aa, a.ac.aca FROM \"%s\" WHERE a.aa in ('8', '9') AND a.ac.aca > 6", tableName);
        String expResultsQuery = "SELECT * FROM VALUES('8', 8),('9', 9)";

        assertDeltaQueryOptimized(
                tableName,
                testQuery,
                expResultsQuery,
                ImmutableMap.of(
                        "a$_$_$aa", multipleValues(VARCHAR, ImmutableList.of(utf8Slice("8"), utf8Slice("9"))),
                        "a$_$_$ac$_$_$aca", Domain.create(
                                SortedRangeSet.copyOf(
                                        INTEGER,
                                        ImmutableList.of(Range.greaterThan(INTEGER, 6L))),
                                false)),
                ImmutableMap.of());
    }

    private void assertDeltaQueryOptimized(
            String tableName,
            String testQuery,
            String expResultsQuery,
            Map<String, Domain> expectedConstraint,
            Map<String, Domain> expectedEnforcedConstraint)
    {
        // verify the plan contains filter pushed down into scan appropriately
        assertPlan(withDereferencePushdownEnabled(),
                testQuery,
                anyTree(tableScanWithConstraints(
                        tableName,
                        expectedConstraint,
                        expectedEnforcedConstraint)));

        assertQuery(testQuery, expResultsQuery);
    }

    /**
     * Utility plan verification method that checks whether the table scan node has given constraint.
     */
    private static PlanMatchPattern tableScanWithConstraints(
            String tableName,
            Map<String, Domain> expectedConstraint,
            Map<String, Domain> expectedEnforcedConstraint)
    {
        return PlanMatchPattern.tableScan(tableName).with(new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof TableScanNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                TableScanNode tableScan = (TableScanNode) node;
                Map<String, Domain> constraint = transform(tableScan.getCurrentConstraint());
                Map<String, Domain> enforcedConstraint = transform(tableScan.getEnforcedConstraint());

                if (!expectedConstraint.equals(constraint) || !expectedEnforcedConstraint.equals(enforcedConstraint)) {
                    return NO_MATCH;
                }

                // Make sure the Delta table handle contain the full constraint
                if (!getConstraintInDeltaTable(tableScan).equals(constraint)) {
                    return NO_MATCH;
                }

                return match();
            }
        });
    }

    private static Map<String, Domain> transform(TupleDomain<ColumnHandle> constraint)
    {
        return constraint.transform(DeltaColumnHandle.class::cast)
                .transform(DeltaColumnHandle::getName)
                .getDomains().get();
    }

    private static Map<String, Domain> getConstraintInDeltaTable(TableScanNode tableScan)
    {
        return ((DeltaTableLayoutHandle) tableScan.getTable().getLayout().get())
                .getPredicate()
                .transform(DeltaColumnHandle::getName)
                .getDomains()
                .get();
    }

    private Session withDereferencePushdownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(DELTA_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
    }
}
