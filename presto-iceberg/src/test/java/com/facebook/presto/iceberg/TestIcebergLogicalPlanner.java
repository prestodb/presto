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
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_DEREFERENCE_ENABLED;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.greaterThan;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.predicate.ValueSet.ofRanges;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.isEntireColumn;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getSynthesizedIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergLogicalPlanner
        extends AbstractTestQueryFramework
{
    protected TestIcebergLogicalPlanner() {}

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of("experimental.pushdown-subfields-enabled", "true"), ImmutableMap.of());
    }

    @Test
    public void testFilterByUnmatchedValueWithFilterPushdown()
    {
        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        String tableName = "test_filter_by_unmatched_value";
        assertUpdate("CREATE TABLE " + tableName + " (a varchar, b integer, r row(c int, d varchar)) WITH(partitioning = ARRAY['a'])");

        // query with normal column filter on empty table
        assertPlan(sessionWithFilterPushdown, "select a, r from " + tableName + " where b = 1001",
                output(values("a", "r")));

        // query with partition column filter on empty table
        assertPlan(sessionWithFilterPushdown, "select b, r from " + tableName + " where a = 'var3'",
                output(values("b", "r")));

        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 1, (1001, 't1')), ('var1', 3, (1003, 't3'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var2', 8, (1008, 't8')), ('var2', 10, (1010, 't10'))", 2);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('var1', 2, (1002, 't2')), ('var1', 9, (1009, 't9'))", 2);

        // query with unmatched normal column filter
        assertPlan(sessionWithFilterPushdown, "select a, r from " + tableName + " where b = 1001",
                output(values("a", "r")));

        // query with unmatched partition column filter
        assertPlan(sessionWithFilterPushdown, "select b, r from " + tableName + " where a = 'var3'",
                output(values("b", "r")));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFiltersWithPushdownDisable()
    {
        // The filter pushdown session property is disabled by default
        Session sessionWithoutFilterPushdown = getQueryRunner().getDefaultSession();

        assertUpdate("CREATE TABLE test_filters_with_pushdown_disable(id int, name varchar, r row(a int, b varchar)) with (partitioning = ARRAY['id'])");

        // Only identity partition column predicates, would be enforced totally by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT name, r FROM test_filters_with_pushdown_disable WHERE id = 10",
                output(exchange(
                        strictTableScan("test_filters_with_pushdown_disable", identityMap("name", "r")))),
                plan -> assertTableLayout(
                        plan,
                        "test_filters_with_pushdown_disable",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "id",
                                        ImmutableList.of()),
                                singleValue(INTEGER, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("id")));

        // Only normal column predicates, would not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, r FROM test_filters_with_pushdown_disable WHERE name = 'adam'",
                output(exchange(project(
                        filter("name='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Only subfield column predicates, would not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT id, name FROM test_filters_with_pushdown_disable WHERE r.a = 10",
                output(exchange(project(
                        filter("r.a=10",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Predicates with identity partition column and normal column, would not be enforced by tableScan
        // TODO: The predicate could be enforced partially by tableScan, so the filterNode could drop it's filter condition `id=10`
        assertPlan(sessionWithoutFilterPushdown, "SELECT name, r FROM test_filters_with_pushdown_disable WHERE id = 10 and name = 'adam'",
                output(exchange(project(
                        filter("id=10 AND name='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        // Predicates with identity partition column and subfield column, would not be enforced by tableScan
        assertPlan(sessionWithoutFilterPushdown, "SELECT name FROM test_filters_with_pushdown_disable WHERE id = 10 and r.b = 'adam'",
                output(exchange(project(
                        filter("id=10 AND r.b='adam'",
                                strictTableScan("test_filters_with_pushdown_disable", identityMap("id", "name", "r")))))));

        assertUpdate("DROP TABLE test_filters_with_pushdown_disable");
    }

    @Test
    public void testFilterPushdown()
    {
        Session sessionWithFilterPushdown = pushdownFilterEnabled();

        // Only domain predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(project(
                        filter("partkey=10",
                                strictTableScan("lineitem", identityMap("linenumber", "partkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("partkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT partkey, linenumber FROM lineitem WHERE partkey = 10",
                output(exchange(
                        project(ImmutableMap.of("partkey", expression("10")),
                                strictTableScan("lineitem", identityMap("linenumber"))))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("partkey")));

        // Remaining predicate is NULL
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(NULL) > 0",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(NULL) > 0",
                output(values("linenumber")));

        // Remaining predicate is always FALSE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey > 10 AND cardinality(ARRAY[1]) > 1",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 OR cardinality(ARRAY[1]) > 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "orderkey",
                                        ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("orderkey")));

        // Remaining predicate is always TRUE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        TRUE_CONSTANT,
                        ImmutableSet.of()));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 AND cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                        "orderkey",
                                        ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        TRUE_CONSTANT,
                        ImmutableSet.of("orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 10 OR cardinality(ARRAY[1]) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        TRUE_CONSTANT,
                        ImmutableSet.of()));

        // TupleDomain predicate is always FALSE
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2",
                output(values("linenumber")));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE orderkey = 1 AND orderkey = 2 AND linenumber % 2 = 1",
                output(values("linenumber")));

        FunctionAndTypeManager functionAndTypeManager = getQueryRunner().getMetadata().getFunctionAndTypeManager();
        FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

        // orderkey % 2 = 1
        RowExpression remainingPredicate = new CallExpression(EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                ImmutableList.of(
                        new CallExpression("mod",
                                functionAndTypeManager.lookupFunction("mod", fromTypes(BIGINT, BIGINT)),
                                BIGINT,
                                ImmutableList.of(
                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                        constant(2))),
                        constant(1)));

        // Only remaining predicate
        assertPlan("SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        remainingPredicate,
                        ImmutableSet.of("orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT orderkey, linenumber FROM lineitem WHERE mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("orderkey", "linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        TupleDomain.all(),
                        remainingPredicate,
                        ImmutableSet.of("orderkey")));

        // A mix of domain and remaining predicates
        assertPlan("SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(project(
                        filter("partkey = 10 AND mod(orderkey, 2) = 1",
                                strictTableScan("lineitem", identityMap("linenumber", "orderkey", "partkey")))))));

        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        remainingPredicate,
                        ImmutableSet.of("partkey", "orderkey")));

        assertPlan(sessionWithFilterPushdown, "SELECT partkey, orderkey, linenumber FROM lineitem WHERE partkey = 10 AND mod(orderkey, 2) = 1",
                output(exchange(
                        project(ImmutableMap.of("partkey", expression("10")),
                                strictTableScan("lineitem", identityMap("orderkey", "linenumber"))))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(new Subfield(
                                "partkey",
                                ImmutableList.of()),
                                singleValue(BIGINT, 10L))),
                        remainingPredicate,
                        ImmutableSet.of("partkey", "orderkey")));

        // (partkey = 10 OR orderkey = 5) AND (partkey = 5 OR orderkey = 10)
        RowExpression remainingPredicateForAndOrCombination = new SpecialFormExpression(AND,
                BOOLEAN,
                ImmutableList.of(
                        new SpecialFormExpression(OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "partkey", BIGINT),
                                                        constant(10))),
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                                        constant(5))))),
                        new SpecialFormExpression(OR,
                                BOOLEAN,
                                ImmutableList.of(
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "orderkey", BIGINT),
                                                        constant(10))),
                                        new CallExpression(EQUAL.name(),
                                                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                                                BOOLEAN,
                                                ImmutableList.of(
                                                        new VariableReferenceExpression(Optional.empty(), "partkey", BIGINT),
                                                        constant(5)))))));

        // A mix of OR, AND in domain predicates
        assertPlan(sessionWithFilterPushdown, "SELECT linenumber FROM lineitem WHERE (partkey = 10 and orderkey = 10) OR (partkey = 5 AND orderkey = 5)",
                output(exchange(strictTableScan("lineitem", identityMap("linenumber")))),
                plan -> assertTableLayout(
                        plan,
                        "lineitem",
                        withColumnDomains(ImmutableMap.of(
                                new Subfield("partkey", ImmutableList.of()),
                                multipleValues(BIGINT, ImmutableList.of(5L, 10L)),
                                new Subfield("orderkey", ImmutableList.of()),
                                multipleValues(BIGINT, ImmutableList.of(5L, 10L)))),
                        remainingPredicateForAndOrCombination,
                        ImmutableSet.of("partkey", "orderkey")));
    }

    @Test
    public void testPartitionPruning()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE test_partition_pruning WITH (partitioning = ARRAY['ds']) AS " +
                "SELECT orderkey, CAST(to_iso8601(date_add('DAY', orderkey % 7, date('2019-11-01'))) AS VARCHAR) AS ds FROM orders WHERE orderkey < 1000");

        Session sessionWithFilterPushdown = pushdownFilterEnabled();
        try {
            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds = '2019-11-01'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) = date('2019-11-01')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", singleValue(VARCHAR, utf8Slice("2019-11-01"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) BETWEEN date('2019-11-02') AND date('2019-11-04')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-01", "2019-11-02", "2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04", "2019-11-05", "2019-11-06", "2019-11-07"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05' AND date(ds) > date('2019-11-02')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds < '2019-11-05' OR ds = '2019-11-07'",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-01", "2019-11-02", "2019-11-03", "2019-11-04", "2019-11-07"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE (ds < '2019-11-05' AND date(ds) > date('2019-11-02')) OR (ds > '2019-11-05' AND ds < '2019-11-07')",
                    anyTree(tableScanWithConstraint("test_partition_pruning", ImmutableMap.of("ds", multipleValues(VARCHAR, utf8Slices("2019-11-03", "2019-11-04", "2019-11-06"))))));

            assertPlan(sessionWithFilterPushdown, "SELECT * FROM test_partition_pruning WHERE ds = '2019-11-01' AND orderkey = 7",
                    anyTree(tableScanWithConstraint(
                            "test_partition_pruning",
                            ImmutableMap.of(
                                    "ds", singleValue(VARCHAR, utf8Slice("2019-11-01")),
                                    "orderkey", singleValue(BIGINT, 7L)))),
                    plan -> assertTableLayout(
                            plan,
                            "test_partition_pruning",
                            withColumnDomains(ImmutableMap.of(new Subfield(
                                            "orderkey",
                                            ImmutableList.of()),
                                    singleValue(BIGINT, 7L))),
                            TRUE_CONSTANT,
                            ImmutableSet.of("orderkey")));
        }
        finally {
            queryRunner.execute("DROP TABLE test_partition_pruning");
        }
    }

    @Test
    public void testParquetDereferencePushDown()
    {
        assertUpdate("CREATE TABLE test_pushdown_nestedcolumn_parquet(" +
                "id bigint, " +
                "x row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))," +
                "y array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double)))) " +
                "with (format = 'PARQUET')");

        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT x.a, mod(x.d.d1, 2) FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.d.d1"));

        assertParquetDereferencePushDown("SELECT x.d, mod(x.d.d1, 2), x.d.d2 FROM test_pushdown_nestedcolumn_parquet",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet WHERE x.b LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        Subfield nestedColumn1 = nestedColumn("x.a");
        String pushdownColumnName1 = pushdownColumnNameForSubfield(nestedColumn1);
        assertParquetDereferencePushDown("SELECT x.a FROM test_pushdown_nestedcolumn_parquet WHERE x.a > 10 AND x.b LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                withColumnDomains(ImmutableMap.of(getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), create(ofRanges(greaterThan(BIGINT, 10L)), false))));

        // Join
        assertPlan(withParquetDereferencePushDownEnabled(), "SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_nestedcolumn_parquet a WHERE l.linenumber = a.id",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScanParquetDeferencePushDowns("test_pushdown_nestedcolumn_parquet", nestedColumnMap("x.a", "x.d.d1"))))));

        assertPlan(withParquetDereferencePushDownEnabled(), "SELECT l.orderkey, x.a, mod(x.d.d1, 2) FROM lineitem l, test_pushdown_nestedcolumn_parquet a WHERE l.linenumber = a.id AND x.a > 10",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem", ImmutableMap.of())),
                                anyTree(tableScanParquetDeferencePushDowns(
                                        "test_pushdown_nestedcolumn_parquet",
                                        nestedColumnMap("x.a", "x.d.d1"),
                                        ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.a"))),
                                        withColumnDomains(ImmutableMap.of(getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), create(ofRanges(greaterThan(BIGINT, 10L)), false))))))));
        // Aggregation
        assertParquetDereferencePushDown("SELECT id, min(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, min(mod(x.a, 3)) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, min(x.a) FILTER (WHERE x.b LIKE 'abc%') FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        assertParquetDereferencePushDown("SELECT id, min(x.a + 1) * avg(x.d.d1) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.d.d1"));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Dereference can't be pushed down, but the subfield pushdown will help in pruning the number of columns to read
        assertPushdownSubfields("SELECT id, arbitrary(x.a) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                ImmutableMap.of("x", toSubfields("x.a")));

        // Dereference can't be pushed down, but the subfield pushdown will help in pruning the number of columns to read
        assertPushdownSubfields("SELECT id, arbitrary(x).d.d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                ImmutableMap.of("x", toSubfields("x.d.d1")));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.d).d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT id, arbitrary(x.d.d2) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d2"));

        // Unnest
        assertParquetDereferencePushDown("SELECT t.a, t.d.d1, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT t.*, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown("SELECT id, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(a, b, c, d)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Legacy unnest
        Session legacyUnnest = Session.builder(withParquetDereferencePushDownEnabled())
                .setSystemProperty("legacy_unnest", "true")
                .build();
        assertParquetDereferencePushDown(legacyUnnest, "SELECT t.y.a, t.y.d.d1, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown(legacyUnnest, "SELECT t.*, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        assertParquetDereferencePushDown(legacyUnnest, "SELECT id, x.a FROM test_pushdown_nestedcolumn_parquet CROSS JOIN UNNEST(y) as t(y)",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a"));

        // Case sensitivity
        assertParquetDereferencePushDown("SELECT x.a, x.b, x.A + 2 FROM test_pushdown_nestedcolumn_parquet WHERE x.B LIKE 'abc%'",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.a", "x.b"));

        // No pass-through nested column pruning
        assertParquetDereferencePushDown("SELECT id, min(x.d).d1 FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        assertParquetDereferencePushDown("SELECT id, min(x.d).d1, min(x.d.d2) FROM test_pushdown_nestedcolumn_parquet GROUP BY 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d"));

        // Test pushdown of filters on dereference columns
        nestedColumn1 = nestedColumn("x.d.d1");
        pushdownColumnName1 = pushdownColumnNameForSubfield(nestedColumn1);
        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1"),
                ImmutableSet.of(pushdownColumnNameForSubfield(nestedColumn("x.d.d1"))),
                withColumnDomains(ImmutableMap.of(
                        getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), singleValue(BIGINT, 1L))));

        Subfield nestedColumn2 = nestedColumn("x.d.d2");
        String pushdownColumnName2 = pushdownColumnNameForSubfield(nestedColumn2);
        assertParquetDereferencePushDown("SELECT id, x.d.d1 FROM test_pushdown_nestedcolumn_parquet WHERE x.d.d1 = 1 and x.d.d2 = 5.0",
                "test_pushdown_nestedcolumn_parquet",
                nestedColumnMap("x.d.d1", "x.d.d2"),
                ImmutableSet.of(
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d1")),
                        pushdownColumnNameForSubfield(nestedColumn("x.d.d2"))),
                withColumnDomains(ImmutableMap.of(
                        getSynthesizedIcebergColumnHandle(pushdownColumnName1, BIGINT, nestedColumn1), singleValue(BIGINT, 1L),
                        getSynthesizedIcebergColumnHandle(pushdownColumnName2, DOUBLE, nestedColumn2), singleValue(DOUBLE, 5.0))));

        assertUpdate("DROP TABLE test_pushdown_nestedcolumn_parquet");
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(String expectedTableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergParquetDereferencePushdownMatcher(expectedDeferencePushDowns, ImmutableSet.of(), TupleDomain.all()));
    }

    private static PlanMatchPattern tableScanParquetDeferencePushDowns(
            String expectedTableName,
            Map<String, Subfield> expectedDeferencePushDowns,
            Set<String> predicateColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergParquetDereferencePushdownMatcher(expectedDeferencePushDowns, predicateColumns, predicate));
    }

    private void assertParquetDereferencePushDown(@Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertParquetDereferencePushDown(withParquetDereferencePushDownEnabled(), query, tableName, expectedDeferencePushDowns);
    }

    private void assertParquetDereferencePushDown(@Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns,
            Set<String> predicateColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        assertPlan(withParquetDereferencePushDownEnabled(), query,
                anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns, predicateColumns, predicate)));
    }

    private void assertParquetDereferencePushDown(Session session, @Language("SQL") String query, String tableName, Map<String, Subfield> expectedDeferencePushDowns)
    {
        assertPlan(session, query, anyTree(tableScanParquetDeferencePushDowns(tableName, expectedDeferencePushDowns)));
    }

    private RowExpression constant(long value)
    {
        return new ConstantExpression(value, BIGINT);
    }

    private static Map<String, String> identityMap(String... values)
    {
        return Arrays.stream(values).collect(toImmutableMap(Functions.identity(), Functions.identity()));
    }

    private void assertTableLayout(Plan plan, String tableName, TupleDomain<Subfield> domainPredicate, RowExpression remainingPredicate, Set<String> predicateColumnNames)
    {
        TableScanNode tableScan = searchFrom(plan.getRoot())
                .where(node -> isTableScanNode(node, tableName))
                .findOnlyElement();

        assertTrue(tableScan.getTable().getLayout().isPresent());
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableScan.getTable().getLayout().get();

        assertEquals(layoutHandle.getPredicateColumns().keySet(), predicateColumnNames);
        assertEquals(layoutHandle.getDomainPredicate(), domainPredicate);
        assertEquals(layoutHandle.getRemainingPredicate(), remainingPredicate);
    }

    private static boolean isTableScanNode(PlanNode node, String tableName)
    {
        return node instanceof TableScanNode && ((IcebergTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getIcebergTableName().getTableName().equals(tableName);
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    private Session withParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "true")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private Session withoutParquetDereferencePushDownEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSHDOWN_DEREFERENCE_ENABLED, "false")
                .setCatalogSessionProperty(ICEBERG_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "false")
                .build();
    }

    private static Set<Subfield> toSubfields(String... subfieldPaths)
    {
        return Arrays.stream(subfieldPaths)
                .map(Subfield::new)
                .collect(toImmutableSet());
    }

    private void assertPushdownSubfields(@Language("SQL") String query, String tableName, Map<String, Set<Subfield>> requiredSubfields)
    {
        assertPlan(withoutParquetDereferencePushDownEnabled(), query, anyTree(tableScan(tableName, requiredSubfields)));
    }

    private static PlanMatchPattern tableScan(String expectedTableName, Map<String, Set<Subfield>> expectedRequiredSubfields)
    {
        return PlanMatchPattern.tableScan(expectedTableName).with(new IcebergTableScanMatcher(expectedRequiredSubfields));
    }

    private static List<Slice> utf8Slices(String... values)
    {
        return Arrays.stream(values).map(Slices::utf8Slice).collect(toImmutableList());
    }

    private static PlanMatchPattern tableScanWithConstraint(String tableName, Map<String, Domain> expectedConstraint)
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
                TupleDomain<String> constraint = tableScan.getCurrentConstraint()
                        .transform(IcebergColumnHandle.class::cast)
                        .transform(IcebergColumnHandle::getName);

                if (!expectedConstraint.equals(constraint.getDomains().get())) {
                    return NO_MATCH;
                }

                return match();
            }
        });
    }

    private static final class IcebergTableScanMatcher
            implements Matcher
    {
        private final Map<String, Set<Subfield>> requiredSubfields;

        private IcebergTableScanMatcher(Map<String, Set<Subfield>> requiredSubfields)
        {
            this.requiredSubfields = requireNonNull(requiredSubfields, "requiredSubfields is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            TableScanNode tableScan = (TableScanNode) node;
            for (ColumnHandle column : tableScan.getAssignments().values()) {
                IcebergColumnHandle icebergColumn = (IcebergColumnHandle) column;
                String columnName = icebergColumn.getName();
                if (requiredSubfields.containsKey(columnName)) {
                    if (!requiredSubfields.get(columnName).equals(ImmutableSet.copyOf(icebergColumn.getRequiredSubfields()))) {
                        return NO_MATCH;
                    }
                }
                else {
                    if (!icebergColumn.getRequiredSubfields().isEmpty()) {
                        return NO_MATCH;
                    }
                }
            }

            return match();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("requiredSubfields", requiredSubfields)
                    .toString();
        }
    }

    private static final class IcebergParquetDereferencePushdownMatcher
            implements Matcher
    {
        private final Map<String, Subfield> dereferenceColumns;
        private final Set<String> predicateColumns;
        private final TupleDomain<IcebergColumnHandle> predicate;

        private IcebergParquetDereferencePushdownMatcher(
                Map<String, Subfield> dereferenceColumns,
                Set<String> predicateColumns,
                TupleDomain<IcebergColumnHandle> predicate)
        {
            this.dereferenceColumns = requireNonNull(dereferenceColumns, "dereferenceColumns is null");
            this.predicateColumns = requireNonNull(predicateColumns, "predicateColumns is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            TableScanNode tableScan = (TableScanNode) node;
            for (ColumnHandle column : tableScan.getAssignments().values()) {
                IcebergColumnHandle icebergColumn = (IcebergColumnHandle) column;
                String columnName = icebergColumn.getName();

                if (dereferenceColumns.containsKey(columnName)) {
                    if (icebergColumn.getColumnType() != SYNTHESIZED ||
                            icebergColumn.getRequiredSubfields().size() != 1 ||
                            !icebergColumn.getRequiredSubfields().get(0).equals(dereferenceColumns.get(columnName))) {
                        return NO_MATCH;
                    }
                    dereferenceColumns.remove(columnName);
                }
                else {
                    if (isPushedDownSubfield(icebergColumn)) {
                        return NO_MATCH;
                    }
                }
            }

            if (!dereferenceColumns.isEmpty()) {
                return NO_MATCH;
            }

            Optional<ConnectorTableLayoutHandle> layout = tableScan.getTable().getLayout();

            if (!layout.isPresent()) {
                return NO_MATCH;
            }

            IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) layout.get();

            TupleDomain<ColumnHandle> tupleDomain = layoutHandle.getDomainPredicate()
                    .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                    .transform(layoutHandle.getPredicateColumns()::get)
                    .transform(ColumnHandle.class::cast);

            Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> columnDomains = tupleDomain.getColumnDomains();
            Set<String> actualPredicateColumns = ImmutableSet.of();
            if (columnDomains.isPresent()) {
                actualPredicateColumns = columnDomains.get().stream()
                        .map(TupleDomain.ColumnDomain::getColumn)
                        .filter(c -> c instanceof IcebergColumnHandle)
                        .map(c -> ((IcebergColumnHandle) c).getName())
                        .collect(Collectors.toSet());
            }

            if (!Objects.equals(actualPredicateColumns, predicateColumns)
                    || !Objects.equals(tupleDomain, predicate)) {
                return NO_MATCH;
            }

            return match();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dereferenceColumns", dereferenceColumns)
                    .add("predicateColumns", predicateColumns)
                    .add("predicate", predicate)
                    .toString();
        }
    }

    private static Map<String, Subfield> nestedColumnMap(String... columns)
    {
        return Arrays.stream(columns).collect(Collectors.toMap(
                column -> pushdownColumnNameForSubfield(nestedColumn(column)),
                TestIcebergLogicalPlanner::nestedColumn));
    }

    private static Subfield nestedColumn(String column)
    {
        return new Subfield(column);
    }
}
