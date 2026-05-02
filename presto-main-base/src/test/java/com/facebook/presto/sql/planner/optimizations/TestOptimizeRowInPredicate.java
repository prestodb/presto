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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_ROW_IN_PREDICATE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractDisjuncts;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOptimizeRowInPredicate
        extends BaseRuleTest
{
    private static final RowType ROW_VARCHAR_BIGINT = RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT));

    private TableHandle tableHandle()
    {
        return new TableHandle(
                new ConnectorId("local"),
                new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                TestingTransactionHandle.create(),
                Optional.empty());
    }

    private static SpecialFormExpression rowIn(VariableReferenceExpression c1, VariableReferenceExpression c2)
    {
        return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                new SpecialFormExpression(ROW_CONSTRUCTOR, ROW_VARCHAR_BIGINT, ImmutableList.of(c1, c2)),
                new SpecialFormExpression(ROW_CONSTRUCTOR, ROW_VARCHAR_BIGINT, ImmutableList.of(
                        new ConstantExpression(Slices.utf8Slice("a"), VARCHAR),
                        new ConstantExpression(1L, BIGINT))),
                new SpecialFormExpression(ROW_CONSTRUCTOR, ROW_VARCHAR_BIGINT, ImmutableList.of(
                        new ConstantExpression(Slices.utf8Slice("b"), VARCHAR),
                        new ConstantExpression(2L, BIGINT)))));
    }

    @Test
    public void testRowInRewriteAddsPerColumnInPredicates()
    {
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    return p.filter(
                            rowIn(c1, c2),
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TpchColumnHandle("c1", VARCHAR),
                                            c2, new TpchColumnHandle("c2", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    List<RowExpression> conjuncts = extractConjuncts(filter.getPredicate());
                    // Expect: c1 IN (a, b), c2 IN (1, 2), OR-of-ANDs
                    assertEquals(conjuncts.size(), 3);
                    assertColumnIn(conjuncts.get(0), "c1", 2);
                    assertColumnIn(conjuncts.get(1), "c2", 2);
                    // Last conjunct is the original OR-of-ANDs (two disjuncts, one per candidate row)
                    assertEquals(extractDisjuncts(conjuncts.get(2)).size(), 2);
                });
    }

    @Test
    public void testRowNotInRewriteAddsPerColumnNotInPredicates()
    {
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    RowExpression notRowIn = p.rowExpression("NOT (ROW(c1, c2) IN (ROW('a', BIGINT '1'), ROW('b', BIGINT '2')))");
                    return p.filter(
                            notRowIn,
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TpchColumnHandle("c1", VARCHAR),
                                            c2, new TpchColumnHandle("c2", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    List<RowExpression> disjuncts = extractDisjuncts(filter.getPredicate());
                    // Expect: NOT(c1 IN ...), NOT(c2 IN ...), NOT(original ROW IN)
                    assertEquals(disjuncts.size(), 3);
                    assertColumnNotIn(disjuncts.get(0), "c1", 2);
                    assertColumnNotIn(disjuncts.get(1), "c2", 2);
                    assertTrue(disjuncts.get(2) instanceof CallExpression);
                    assertEquals(((CallExpression) disjuncts.get(2)).getDisplayName().toLowerCase(), "not");
                });
    }

    @Test
    public void testNoRewriteWhenSessionPropertyDisabled()
    {
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "false")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    return p.filter(
                            rowIn(c1, c2),
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TpchColumnHandle("c1", VARCHAR),
                                            c2, new TpchColumnHandle("c2", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    // Predicate untouched — still the original ROW IN
                    assertTrue(filter.getPredicate() instanceof SpecialFormExpression);
                    assertEquals(((SpecialFormExpression) filter.getPredicate()).getForm(), IN);
                });
    }

    @Test
    public void testNonRowConstructorTargetIsNotRewritten()
    {
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", BIGINT);
                    RowExpression scalarIn = new SpecialFormExpression(IN, BOOLEAN, ImmutableList.of(
                            c1,
                            new ConstantExpression(1L, BIGINT),
                            new ConstantExpression(2L, BIGINT)));
                    return p.filter(
                            scalarIn,
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1),
                                    ImmutableMap.of(c1, new TpchColumnHandle("c1", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    SpecialFormExpression in = (SpecialFormExpression) filter.getPredicate();
                    assertEquals(in.getForm(), IN);
                    // First arg is the scalar variable, not a ROW constructor — predicate must be unchanged
                    assertFalse(in.getArguments().get(0) instanceof SpecialFormExpression);
                });
    }

    @Test
    public void testRewriteAppliesUnderTopLevelAndConjunction()
    {
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    VariableReferenceExpression c3 = p.variable("c3", BIGINT);

                    RowExpression sideCondition = p.rowExpression("c3 > BIGINT '0'");
                    RowExpression conjunction = new SpecialFormExpression(SpecialFormExpression.Form.AND, BOOLEAN, ImmutableList.of(rowIn(c1, c2), sideCondition));

                    return p.filter(
                            conjunction,
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1, c2, c3),
                                    ImmutableMap.of(
                                            c1, new TpchColumnHandle("c1", VARCHAR),
                                            c2, new TpchColumnHandle("c2", BIGINT),
                                            c3, new TpchColumnHandle("c3", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    List<RowExpression> conjuncts = extractConjuncts(filter.getPredicate());
                    // After rewrite: c1 IN (...), c2 IN (...), OR-of-ANDs, c3 > 0  → 4 conjuncts
                    assertEquals(conjuncts.size(), 4);
                    assertColumnIn(conjuncts.get(0), "c1", 2);
                    assertColumnIn(conjuncts.get(1), "c2", 2);
                });
    }

    @Test
    public void testRewriteAppliesUnderTopLevelOrDisjunction()
    {
        // Verify the rewriter recurses into OR disjuncts (not just AND), so a ROW IN nested
        // under OR still gets rewritten. Pre-refactor we missed this case.
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    VariableReferenceExpression c3 = p.variable("c3", BIGINT);

                    RowExpression sideCondition = p.rowExpression("c3 > BIGINT '0'");
                    RowExpression disjunction = new SpecialFormExpression(SpecialFormExpression.Form.OR, BOOLEAN, ImmutableList.of(rowIn(c1, c2), sideCondition));

                    return p.filter(
                            disjunction,
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(c1, c2, c3),
                                    ImmutableMap.of(
                                            c1, new TpchColumnHandle("c1", VARCHAR),
                                            c2, new TpchColumnHandle("c2", BIGINT),
                                            c3, new TpchColumnHandle("c3", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    // Top-level is still OR, but the first disjunct must be the rewritten ROW IN
                    // (an AND of per-column INs + OR-of-ANDs), not the original IN form.
                    List<RowExpression> disjuncts = extractDisjuncts(filter.getPredicate());
                    assertEquals(disjuncts.size(), 2);
                    List<RowExpression> rewrittenConjuncts = extractConjuncts(disjuncts.get(0));
                    // c1 IN (...), c2 IN (...), OR-of-ANDs
                    assertEquals(rewrittenConjuncts.size(), 3);
                    assertColumnIn(rewrittenConjuncts.get(0), "c1", 2);
                    assertColumnIn(rewrittenConjuncts.get(1), "c2", 2);
                });
    }

    @Test
    public void testRewriteEnablesPerColumnDomainExtractionForPartitionKeys()
    {
        // The whole reason the rewrite exists: domain translator extracts NO per-column constraints
        // from a raw ROW IN, but extracts both columns once the per-column IN predicates are added.
        // This is what unlocks partition pruning at PickTableLayout.
        VariableReferenceExpression pk1 = new VariableReferenceExpression(Optional.empty(), "pk1", VARCHAR);
        VariableReferenceExpression pk2 = new VariableReferenceExpression(Optional.empty(), "pk2", BIGINT);
        SpecialFormExpression originalRowIn = rowIn(pk1, pk2);

        RowExpressionDomainTranslator domainTranslator = new RowExpressionDomainTranslator(tester().getMetadata());

        ExtractionResult<VariableReferenceExpression> before = domainTranslator.fromPredicate(
                tester().getSession().toConnectorSession(), originalRowIn);
        assertTrue(before.getTupleDomain().isAll(), "ROW IN should yield no per-column TupleDomain pre-rewrite");

        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression v1 = p.variable("pk1", VARCHAR);
                    VariableReferenceExpression v2 = p.variable("pk2", BIGINT);
                    return p.filter(
                            rowIn(v1, v2),
                            p.tableScan(
                                    tableHandle(),
                                    ImmutableList.of(v1, v2),
                                    ImmutableMap.of(
                                            v1, new TpchColumnHandle("pk1", VARCHAR),
                                            v2, new TpchColumnHandle("pk2", BIGINT))));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    ExtractionResult<VariableReferenceExpression> after = domainTranslator.fromPredicate(
                            tester().getSession().toConnectorSession(), filter.getPredicate());
                    TupleDomain<VariableReferenceExpression> tupleDomain = after.getTupleDomain();
                    assertFalse(tupleDomain.isAll(), "Rewritten predicate should yield non-trivial TupleDomain");

                    Map<VariableReferenceExpression, Domain> domains = tupleDomain.getDomains().get();
                    VariableReferenceExpression rewrittenPk1 = domains.keySet().stream()
                            .filter(v -> v.getName().equals("pk1")).findFirst().orElseThrow(() -> new AssertionError("pk1 missing"));
                    VariableReferenceExpression rewrittenPk2 = domains.keySet().stream()
                            .filter(v -> v.getName().equals("pk2")).findFirst().orElseThrow(() -> new AssertionError("pk2 missing"));

                    Domain pk1Domain = domains.get(rewrittenPk1);
                    assertTrue(pk1Domain.getValues().containsValue(Slices.utf8Slice("a")));
                    assertTrue(pk1Domain.getValues().containsValue(Slices.utf8Slice("b")));

                    Domain pk2Domain = domains.get(rewrittenPk2);
                    assertTrue(pk2Domain.getValues().containsValue(1L));
                    assertTrue(pk2Domain.getValues().containsValue(2L));
                });
    }

    @Test
    public void testRewriteSkippedWhenFilterIsNotOnTableScan()
    {
        // Filter sits on a Values node (not a TableScan / Project chain) — rewrite must not fire,
        // since per-column predicates have no partition-pruning benefit there.
        tester().assertThat(new OptimizeRowInPredicate(tester().getMetadata()))
                .setSystemProperty(OPTIMIZE_ROW_IN_PREDICATE, "true")
                .on(p -> {
                    VariableReferenceExpression c1 = p.variable("c1", VARCHAR);
                    VariableReferenceExpression c2 = p.variable("c2", BIGINT);
                    return p.filter(
                            rowIn(c1, c2),
                            p.values(c1, c2));
                })
                .validates(plan -> {
                    FilterNode filter = (FilterNode) plan.getRoot();
                    // Predicate untouched
                    assertTrue(filter.getPredicate() instanceof SpecialFormExpression);
                    assertEquals(((SpecialFormExpression) filter.getPredicate()).getForm(), IN);
                });
    }

    private static void assertColumnIn(RowExpression expr, String varName, int expectedValueCount)
    {
        assertTrue(expr instanceof SpecialFormExpression, expr + " is not a SpecialFormExpression");
        SpecialFormExpression in = (SpecialFormExpression) expr;
        assertEquals(in.getForm(), IN);
        List<RowExpression> args = in.getArguments();
        assertEquals(args.size(), 1 + expectedValueCount);
        assertTrue(args.get(0) instanceof VariableReferenceExpression);
        assertEquals(((VariableReferenceExpression) args.get(0)).getName(), varName);
    }

    private static void assertColumnNotIn(RowExpression expr, String varName, int expectedValueCount)
    {
        assertTrue(expr instanceof CallExpression, expr + " is not a CallExpression");
        CallExpression notExpr = (CallExpression) expr;
        assertEquals(notExpr.getDisplayName().toLowerCase(), "not");
        assertEquals(notExpr.getArguments().size(), 1);
        assertColumnIn(notExpr.getArguments().get(0), varName, expectedValueCount);
    }
}
