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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRewriteRowConstructorInToDisjunction
{
    private RuleTester tester;
    private TableHandle partitionedTableHandle;
    private TableHandle nonPartitionedTableHandle;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                new PartitionedMockConnectorFactory());
        String connectorId = tester.getCurrentConnectorId().toString();

        partitionedTableHandle = new TableHandle(
                tester.getCurrentConnectorId(),
                new PartitionedMockTableHandle("partitioned_table"),
                TestingTransactionHandle.create(),
                Optional.empty());

        nonPartitionedTableHandle = new TableHandle(
                tester.getCurrentConnectorId(),
                new PartitionedMockTableHandle("non_partitioned_table"),
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testRewriteEnablesPartitionPruningViaTupleDomain()
    {
        Metadata metadata = tester.getMetadata();
        RewriteRowConstructorInToDisjunction rule = new RewriteRowConstructorInToDisjunction(metadata);
        RowExpressionDomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);

        VariableReferenceExpression pk1 = new VariableReferenceExpression(Optional.empty(), "pk1", VARCHAR);
        VariableReferenceExpression pk2 = new VariableReferenceExpression(Optional.empty(), "pk2", BIGINT);

        RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

        // Original predicate: ROW(pk1, pk2) IN (ROW('a', 1), ROW('b', 2))
        RowExpression originalPredicate = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, pk2)),
                new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                        new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                        new ConstantExpression(1L, BIGINT))),
                new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                        new ConstantExpression(Slices.utf8Slice("b"), VARCHAR),
                        new ConstantExpression(2L, BIGINT)))));

        // ============================================================
        // BEFORE rewrite: domain translator cannot extract TupleDomain
        // ============================================================
        ExtractionResult<VariableReferenceExpression> beforeResult = domainTranslator.fromPredicate(
                tester.getSession().toConnectorSession(), originalPredicate);

        System.out.println("=== Partition Pruning TupleDomain Demo ===");
        System.out.println();
        System.out.println("BEFORE rewrite (PickTableLayout sees this):");
        System.out.println("  Input predicate:       " + originalPredicate);
        System.out.println("  Extracted TupleDomain:  " + beforeResult.getTupleDomain());
        System.out.println("  Remaining expression:   " + beforeResult.getRemainingExpression());
        System.out.println("  >> TupleDomain is ALL — NO partition pruning possible!");
        System.out.println();

        assertTrue(beforeResult.getTupleDomain().isAll(),
                "BEFORE rewrite: TupleDomain should be ALL (no per-column domains extracted)");
        assertEquals(beforeResult.getRemainingExpression(), originalPredicate,
                "BEFORE rewrite: entire predicate should remain as-is (unsupported by domain translator)");

        // ============================================================
        // Apply the rewrite rule
        // ============================================================
        PlanNode result = tester.assertThat(rule)
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> p.filter(
                        originalPredicate,
                        p.tableScan(
                                partitionedTableHandle,
                                ImmutableList.of(pk1, pk2),
                                ImmutableMap.of(
                                        pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                        pk2, new TestingColumnHandle("pk2", 1, BIGINT)))))
                .get();

        FilterNode filterNode = (FilterNode) result;
        RowExpression rewrittenPredicate = filterNode.getPredicate();

        // ============================================================
        // AFTER rewrite: domain translator extracts per-column domains
        // ============================================================
        ExtractionResult<VariableReferenceExpression> afterResult = domainTranslator.fromPredicate(
                tester.getSession().toConnectorSession(), rewrittenPredicate);

        TupleDomain<VariableReferenceExpression> tupleDomain = afterResult.getTupleDomain();
        Map<VariableReferenceExpression, Domain> domains = tupleDomain.getDomains().get();

        System.out.println("AFTER rewrite (PickTableLayout sees this):");
        System.out.println("  Rewritten predicate:   " + rewrittenPredicate);
        System.out.println("  Extracted TupleDomain:  " + tupleDomain);
        System.out.println("  Per-column domains:");
        for (Map.Entry<VariableReferenceExpression, Domain> entry : domains.entrySet()) {
            System.out.println("    " + entry.getKey().getName() + " → " + entry.getValue());
        }
        System.out.println("  Remaining expression:   " + afterResult.getRemainingExpression());
        System.out.println("  >> Per-column domains extracted — partition pruning ENABLED!");
        System.out.println("=============================================");

        assertTrue(domains.containsKey(pk1), "AFTER rewrite: TupleDomain should contain pk1 domain");
        assertTrue(domains.containsKey(pk2), "AFTER rewrite: TupleDomain should contain pk2 domain");

        // pk1 should be constrained to {'a', 'b'}
        Domain pk1Domain = domains.get(pk1);
        assertTrue(pk1Domain.getValues().containsValue(Slices.utf8Slice("a")));
        assertTrue(pk1Domain.getValues().containsValue(Slices.utf8Slice("b")));

        // pk2 should be constrained to {1, 2}
        Domain pk2Domain = domains.get(pk2);
        assertTrue(pk2Domain.getValues().containsValue(1L));
        assertTrue(pk2Domain.getValues().containsValue(2L));
    }

    @Test
    public void testRewriteRowInWithAllPartitionKeys()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression pk2 = p.variable("pk2", BIGINT);
                    VariableReferenceExpression data = p.variable("data", VARCHAR);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, pk2)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(1L, BIGINT))),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("b"), VARCHAR),
                                    new ConstantExpression(2L, BIGINT)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1, pk2, data),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                            pk2, new TestingColumnHandle("pk2", 1, BIGINT),
                                            data, new TestingColumnHandle("data", 2, VARCHAR))));
                })
                .get();

        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();

        // Should be: (pk1 = 'a' AND pk2 = 1) OR (pk1 = 'b' AND pk2 = 2)
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression orExpr = (SpecialFormExpression) predicate;
        assertEquals(orExpr.getForm(), OR);
        assertEquals(orExpr.getArguments().size(), 2);

        // First disjunct: pk1 = 'a' AND pk2 = 1
        RowExpression firstDisjunct = orExpr.getArguments().get(0);
        assertTrue(firstDisjunct instanceof SpecialFormExpression);
        SpecialFormExpression andExpr1 = (SpecialFormExpression) firstDisjunct;
        assertEquals(andExpr1.getForm(), AND);
        assertEquals(andExpr1.getArguments().size(), 2);

        // Each conjunct should be an EQUAL CallExpression
        assertTrue(andExpr1.getArguments().get(0) instanceof CallExpression);
        assertTrue(andExpr1.getArguments().get(1) instanceof CallExpression);
    }

    @Test
    public void testSingleCandidateRewrite()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression pk2 = p.variable("pk2", BIGINT);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, pk2)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("x"), VARCHAR),
                                    new ConstantExpression(42L, BIGINT)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1, pk2),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                            pk2, new TestingColumnHandle("pk2", 1, BIGINT))));
                })
                .get();

        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();

        // Single candidate: pk1 = 'x' AND pk2 = 42
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression andExpr = (SpecialFormExpression) predicate;
        assertEquals(andExpr.getForm(), AND);
        assertEquals(andExpr.getArguments().size(), 2);
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "false")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression pk2 = p.variable("pk2", BIGINT);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, pk2)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(1L, BIGINT)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1, pk2),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                            pk2, new TestingColumnHandle("pk2", 1, BIGINT))));
                })
                .doesNotFire();
    }

    @Test
    public void testFiresWhenSomeFieldsArePartitionKeys()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression data = p.variable("data", VARCHAR);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR));

                    // ROW(pk1, data) IN (...) — pk1 IS a partition key, data is NOT
                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, data)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(Slices.utf8Slice("val1"), VARCHAR))),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("b"), VARCHAR),
                                    new ConstantExpression(Slices.utf8Slice("val2"), VARCHAR)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1, data),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                            data, new TestingColumnHandle("data", 2, VARCHAR))));
                })
                .get();

        // Should rewrite because pk1 is a partition key
        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();

        // Should be: (pk1 = 'a' AND data = 'val1') OR (pk1 = 'b' AND data = 'val2')
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression orExpr = (SpecialFormExpression) predicate;
        assertEquals(orExpr.getForm(), OR);
        assertEquals(orExpr.getArguments().size(), 2);
    }

    @Test
    public void testFiresWhenNoFieldsArePartitionKeys()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression data = p.variable("data", VARCHAR);
                    VariableReferenceExpression other = p.variable("other", VARCHAR);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR));

                    // ROW(data, other) IN (...) — neither is a partition key, but still rewrites
                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(data, other)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(Slices.utf8Slice("b"), VARCHAR)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(data, other),
                                    ImmutableMap.of(
                                            data, new TestingColumnHandle("data", 2, VARCHAR),
                                            other, new TestingColumnHandle("other", 3, VARCHAR))));
                })
                .get();

        // Should rewrite: (data = 'a' AND other = 'b')
        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression andExpr = (SpecialFormExpression) predicate;
        assertEquals(andExpr.getForm(), AND);
        assertEquals(andExpr.getArguments().size(), 2);
    }

    @Test
    public void testFiresForNonPartitionedTable()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression col1 = p.variable("col1", VARCHAR);
                    VariableReferenceExpression col2 = p.variable("col2", BIGINT);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(col1, col2)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(1L, BIGINT)))));

                    return p.filter(
                            rowIn,
                            p.tableScan(
                                    nonPartitionedTableHandle,
                                    ImmutableList.of(col1, col2),
                                    ImmutableMap.of(
                                            col1, new TestingColumnHandle("col1", 0, VARCHAR),
                                            col2, new TestingColumnHandle("col2", 1, BIGINT))));
                })
                .get();

        // Should rewrite: (col1 = 'a' AND col2 = 1)
        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression andExpr = (SpecialFormExpression) predicate;
        assertEquals(andExpr.getForm(), AND);
        assertEquals(andExpr.getArguments().size(), 2);
    }

    @Test
    public void testDoesNotFireForNonRowIn()
    {
        tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);

                    // Simple col IN ('a', 'b') — no ROW constructor
                    RowExpression simpleIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            pk1,
                            new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                            new ConstantExpression(Slices.utf8Slice("b"), VARCHAR)));

                    return p.filter(
                            simpleIn,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR))));
                })
                .doesNotFire();
    }

    @Test
    public void testRowInEmbeddedInAndPredicate()
    {
        PlanNode result = tester.assertThat(new RewriteRowConstructorInToDisjunction(tester.getMetadata()))
                .setSystemProperty(REWRITE_ROW_CONSTRUCTOR_IN_TO_DISJUNCTION, "true")
                .on(p -> {
                    VariableReferenceExpression pk1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression pk2 = p.variable("pk2", BIGINT);
                    VariableReferenceExpression data = p.variable("data", VARCHAR);

                    RowType rowType = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

                    RowExpression rowIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(pk1, pk2)),
                            new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.of(
                                    new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                                    new ConstantExpression(1L, BIGINT)))));

                    // data IS NOT NULL
                    RowExpression isNotNull = new SpecialFormExpression(
                            SpecialFormExpression.Form.IS_NULL, BOOLEAN, ImmutableList.of(data));

                    // ROW(...) IN (...) AND data IS NOT NULL
                    RowExpression combined = new SpecialFormExpression(
                            SpecialFormExpression.Form.AND, BOOLEAN, ImmutableList.of(rowIn, isNotNull));

                    return p.filter(
                            combined,
                            p.tableScan(
                                    partitionedTableHandle,
                                    ImmutableList.of(pk1, pk2, data),
                                    ImmutableMap.of(
                                            pk1, new TestingColumnHandle("pk1", 0, VARCHAR),
                                            pk2, new TestingColumnHandle("pk2", 1, BIGINT),
                                            data, new TestingColumnHandle("data", 2, VARCHAR))));
                })
                .get();

        assertTrue(result instanceof FilterNode);
        FilterNode filterNode = (FilterNode) result;
        RowExpression predicate = filterNode.getPredicate();

        // Should be: (pk1 = 'a' AND pk2 = 1) AND (data IS NOT NULL)
        assertTrue(predicate instanceof SpecialFormExpression);
        SpecialFormExpression topAnd = (SpecialFormExpression) predicate;
        assertEquals(topAnd.getForm(), AND);
        assertEquals(topAnd.getArguments().size(), 2);

        // One argument should be the rewritten AND (pk1='a' AND pk2=1),
        // the other should be IS_NULL(data)
        boolean foundEqualities = false;
        boolean foundIsNull = false;
        for (RowExpression arg : topAnd.getArguments()) {
            if (arg instanceof SpecialFormExpression) {
                SpecialFormExpression sf = (SpecialFormExpression) arg;
                if (sf.getForm() == AND) {
                    foundEqualities = true;
                    assertEquals(sf.getArguments().size(), 2);
                    assertTrue(sf.getArguments().get(0) instanceof CallExpression);
                    assertTrue(sf.getArguments().get(1) instanceof CallExpression);
                }
                else if (sf.getForm() == SpecialFormExpression.Form.IS_NULL) {
                    foundIsNull = true;
                }
            }
        }
        assertTrue(foundEqualities, "Expected rewritten equality conjuncts");
        assertTrue(foundIsNull, "Expected IS_NULL predicate to be preserved");
    }

    // ========== Custom Connector Factory for partition metadata ==========

    private static class PartitionedMockTableHandle
            implements ConnectorTableHandle
    {
        private final String tableName;

        PartitionedMockTableHandle(String tableName)
        {
            this.tableName = tableName;
        }

        public String getTableName()
        {
            return tableName;
        }
    }

    private static class PartitionedMockConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "partitioned_mock";
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TestingHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return new ConnectorTransactionHandle() {};
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return new PartitionedMockMetadata();
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TpchSplitManager(context.getNodeManager(), 1);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new TpchRecordSetProvider();
                }
            };
        }
    }

    private static class PartitionedMockMetadata
            implements ConnectorMetadata
    {
        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
        {
            return new PartitionedMockTableHandle(tableName.getTableName());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
        {
            PartitionedMockTableHandle mockTable = (PartitionedMockTableHandle) table;
            if ("partitioned_table".equals(mockTable.getTableName())) {
                return new ConnectorTableMetadata(
                        new SchemaTableName("test_schema", "partitioned_table"),
                        ImmutableList.of(
                                ColumnMetadata.builder().setName("pk1").setType(VARCHAR).build(),
                                ColumnMetadata.builder().setName("pk2").setType(BIGINT).build(),
                                ColumnMetadata.builder().setName("data").setType(VARCHAR).build(),
                                ColumnMetadata.builder().setName("other").setType(VARCHAR).build()),
                        ImmutableMap.of("partitioned_by", ImmutableList.of("pk1", "pk2")));
            }
            return new ConnectorTableMetadata(
                    new SchemaTableName("test_schema", mockTable.getTableName()),
                    ImmutableList.of(
                            ColumnMetadata.builder().setName("col1").setType(VARCHAR).build(),
                            ColumnMetadata.builder().setName("col2").setType(BIGINT).build()),
                    ImmutableMap.of());
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
        {
            return ImmutableList.of(
                    new SchemaTableName("test_schema", "partitioned_table"),
                    new SchemaTableName("test_schema", "non_partitioned_table"));
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            PartitionedMockTableHandle mockTable = (PartitionedMockTableHandle) tableHandle;
            if ("partitioned_table".equals(mockTable.getTableName())) {
                return ImmutableMap.of(
                        "pk1", new TestingColumnHandle("pk1", 0, VARCHAR),
                        "pk2", new TestingColumnHandle("pk2", 1, BIGINT),
                        "data", new TestingColumnHandle("data", 2, VARCHAR),
                        "other", new TestingColumnHandle("other", 3, VARCHAR));
            }
            return ImmutableMap.of(
                    "col1", new TestingColumnHandle("col1", 0, VARCHAR),
                    "col2", new TestingColumnHandle("col2", 1, BIGINT));
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            TestingColumnHandle col = (TestingColumnHandle) columnHandle;
            return ColumnMetadata.builder().setName(col.getName()).setType(col.getType()).build();
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            return ImmutableMap.of();
        }

        @Override
        public ConnectorTableLayoutResult getTableLayoutForConstraint(
                ConnectorSession session,
                ConnectorTableHandle table,
                Constraint<ColumnHandle> constraint,
                Optional<Set<ColumnHandle>> desiredColumns)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return ImmutableList.of("test_schema");
        }
    }
}
