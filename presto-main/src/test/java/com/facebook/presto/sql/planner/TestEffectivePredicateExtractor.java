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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.count;
import static com.facebook.presto.sql.planner.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.sql.planner.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestEffectivePredicateExtractor
{
    private static final TableHandle DUAL_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.empty());

    private static final TableHandle DUAL_TABLE_HANDLE_WITH_LAYOUT = new TableHandle(
            new ConnectorId("test"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));

    private static final VariableReferenceExpression AV = new VariableReferenceExpression("a", BIGINT);
    private static final VariableReferenceExpression BV = new VariableReferenceExpression("b", BIGINT);
    private static final VariableReferenceExpression CV = new VariableReferenceExpression("c", BIGINT);
    private static final VariableReferenceExpression DV = new VariableReferenceExpression("d", BIGINT);
    private static final VariableReferenceExpression EV = new VariableReferenceExpression("e", BIGINT);
    private static final VariableReferenceExpression FV = new VariableReferenceExpression("f", BIGINT);
    private static final VariableReferenceExpression GV = new VariableReferenceExpression("g", BIGINT);
    private static final Expression AE = new SymbolReference(AV.getName());
    private static final Expression BE = new SymbolReference(BV.getName());
    private static final Expression CE = new SymbolReference(CV.getName());
    private static final Expression DE = new SymbolReference(DV.getName());
    private static final Expression EE = new SymbolReference(EV.getName());
    private static final Expression FE = new SymbolReference(FV.getName());
    private static final Expression GE = new SymbolReference(GV.getName());

    private static final TypeProvider types = TypeProvider.fromVariables(ImmutableList.of(AV, BV, CV, DV, EV, FV, GV));
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(new ExpressionDomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde())));

    private Map<VariableReferenceExpression, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;
    private ExpressionIdentityNormalizer expressionNormalizer;

    @BeforeMethod
    public void setUp()
    {
        scanAssignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(AV, new TestingColumnHandle("a"))
                .put(BV, new TestingColumnHandle("b"))
                .put(CV, new TestingColumnHandle("c"))
                .put(DV, new TestingColumnHandle("d"))
                .put(EV, new TestingColumnHandle("e"))
                .put(FV, new TestingColumnHandle("f"))
                .build();

        Map<VariableReferenceExpression, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV, DV, EV, FV)));
        baseTableScan = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all());

        expressionNormalizer = new ExpressionIdentityNormalizer();
    }

    @Test
    public void testAggregation()
    {
        PlanNode node = new AggregationNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, DE),
                                equals(BE, EE),
                                equals(CE, FE),
                                lessThan(DE, bigintLiteral(10)),
                                lessThan(CE, DE),
                                greaterThan(AE, bigintLiteral(2)),
                                equals(EE, FE))),
                ImmutableMap.of(
                        CV, count(metadata.getFunctionManager()),
                        DV, count(metadata.getFunctionManager())),
                singleGroupingSet(ImmutableList.of(AV, BV, CV)),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Rewrite in terms of group by variables
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(AE, bigintLiteral(10)),
                        lessThan(BE, AE),
                        greaterThan(AE, bigintLiteral(2)),
                        equals(BE, CE)));
    }

    @Test
    public void testGroupByEmpty()
    {
        PlanNode node = new AggregationNode(
                newId(),
                filter(baseTableScan, FALSE_LITERAL),
                ImmutableMap.of(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        assertEquals(effectivePredicate, TRUE_LITERAL);
    }

    @Test
    public void testFilter()
    {
        PlanNode node = filter(baseTableScan,
                and(
                        greaterThan(AE, new FunctionCall(QualifiedName.of("rand"), ImmutableList.of())),
                        lessThan(BE, bigintLiteral(10))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Non-deterministic functions should be purged
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, bigintLiteral(10))));
    }

    @Test
    public void testProject()
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                assignment(DV, AE, EV, CE));

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Rewrite in terms of project output variables
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(DE, bigintLiteral(10)),
                        equals(DE, EE)));
    }

    @Test
    public void testTopN()
    {
        PlanNode node = new TopNNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1, new OrderingScheme(ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_LAST))), TopNNode.Step.PARTIAL);

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testLimit()
    {
        PlanNode node = new LimitNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                1,
                FINAL);

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testSort()
    {
        PlanNode node = new SortNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new OrderingScheme(ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_LAST))));

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testWindow()
    {
        PlanNode node = new WindowNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, bigintLiteral(10)))),
                new WindowNode.Specification(
                        ImmutableList.of(AV),
                        Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_LAST))))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, bigintLiteral(10))));
    }

    @Test
    public void testTableScan()
    {
        // Effective predicate is True if there is no effective predicate
        Map<VariableReferenceExpression, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV, DV)));
        PlanNode node = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all());
        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

        node = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none(),
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(node, types);
        assertEquals(effectivePredicate, FALSE_LITERAL);

        node = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(AV), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(node, types);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        scanAssignments.get(AV), Domain.singleValue(BIGINT, 1L),
                        scanAssignments.get(BV), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(node, types);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(2L), BE), equals(bigintLiteral(1L), AE)));

        node = new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all());
        effectivePredicate = effectivePredicateExtractor.extract(node, types);
        assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);
    }

    @Test
    public void testUnion()
    {
        ImmutableListMultimap<VariableReferenceExpression, VariableReferenceExpression> variableMapping = ImmutableListMultimap.of(AV, BV, AV, CV, AV, EV);
        PlanNode node = new UnionNode(newId(),
                ImmutableList.of(
                        filter(baseTableScan, greaterThan(AE, bigintLiteral(10))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                        filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100))))),
                variableMapping);

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Only the common conjuncts can be inferred through a Union
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(greaterThan(AE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(AV, DV));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(BV, EV));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.of(castToRowExpression(lessThanOrEqual(BE, EE))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // All predicates having output variable should be carried through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        equals(AE, DE),
                        equals(BE, EE),
                        lessThanOrEqual(BE, EE)));
    }

    @Test
    public void testInnerJoinPropagatesPredicatesViaEquiConditions()
    {
        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, equals(AE, bigintLiteral(10)));

        // predicates on "a" column should be propagated to output variables via join equi conditions
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                left,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(AV, DV)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(rightScan.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        assertEquals(
                normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DE, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoinWithFalseFilter()
    {
        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new JoinNode.EquiJoinClause(AV, DV)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftScan.getOutputVariables())
                        .addAll(rightScan.getOutputVariables())
                        .build(),
                Optional.of(castToRowExpression(FALSE_LITERAL)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        assertEquals(effectivePredicate, FALSE_LITERAL);
    }

    @Test
    public void testLeftJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(AV, DV));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(BV, EV));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // All right side variables having output variables should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(DE, EE), and(isNull(DE), isNull(EE))),
                        or(lessThan(FE, bigintLiteral(100)), isNull(FE)),
                        or(equals(AE, DE), isNull(DE)),
                        or(equals(BE, EE), isNull(EE))));
    }

    @Test
    public void testLeftJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(AV, DV));

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan, FALSE_LITERAL);
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // False literal on the right side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        or(equals(AE, DE), isNull(DE))));
    }

    @Test
    public void testRightJoin()
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(AV, DV));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(BV, EV));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BE, AE),
                        lessThan(CE, bigintLiteral(10)),
                        equals(GE, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // All left side variables should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(or(lessThan(BE, AE), and(isNull(BE), isNull(AE))),
                        or(lessThan(CE, bigintLiteral(10)), isNull(CE)),
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE)),
                        or(equals(BE, EE), isNull(BE))));
    }

    @Test
    public void testRightJoinWithFalseInner()
    {
        List<JoinNode.EquiJoinClause> criteria = ImmutableList.of(new JoinNode.EquiJoinClause(AV, DV));

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, FALSE_LITERAL);
        FilterNode right = filter(rightScan,
                and(
                        equals(DE, EE),
                        lessThan(FE, bigintLiteral(100))));
        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // False literal on the left side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DE, EE),
                        lessThan(FE, bigintLiteral(100)),
                        or(equals(AE, DE), isNull(AE))));
    }

    @Test
    public void testSemiJoin()
    {
        PlanNode node = new SemiJoinNode(newId(),
                filter(baseTableScan, and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))),
                filter(baseTableScan, greaterThan(AE, bigintLiteral(5))),
                AV, BV, CV,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Expression effectivePredicate = effectivePredicateExtractor.extract(node, types);

        // Currently, only pull predicates through the source plan
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(and(greaterThan(AE, bigintLiteral(10)), lessThan(AE, bigintLiteral(100)))));
    }

    private static TableScanNode tableScanNode(Map<VariableReferenceExpression, ColumnHandle> scanAssignments)
    {
        return new TableScanNode(
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                TupleDomain.all(),
                TupleDomain.all());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static FilterNode filter(PlanNode source, Expression predicate)
    {
        return new FilterNode(newId(), source, castToRowExpression(predicate));
    }

    private static Expression bigintLiteral(long number)
    {
        if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE) {
            return new GenericLiteral("BIGINT", String.valueOf(number));
        }
        return new LongLiteral(String.valueOf(number));
    }

    private static ComparisonExpression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, expression1, expression2);
    }

    private static ComparisonExpression lessThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, expression1, expression2);
    }

    private static ComparisonExpression lessThanOrEqual(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private static ComparisonExpression greaterThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, expression1, expression2);
    }

    private static IsNullPredicate isNull(Expression expression)
    {
        return new IsNullPredicate(expression);
    }

    private Set<Expression> normalizeConjuncts(Expression... conjuncts)
    {
        return normalizeConjuncts(Arrays.asList(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Collection<Expression> conjuncts)
    {
        return normalizeConjuncts(combineConjuncts(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Expression predicate)
    {
        // Normalize the predicate by identity so that the EqualityInference will produce stable rewrites in this test
        // and thereby produce comparable Sets of conjuncts from this method.
        predicate = expressionNormalizer.normalize(predicate);

        // Equality inference rewrites and equality generation will always be stable across multiple runs in the same JVM
        EqualityInference inference = EqualityInference.createEqualityInference(predicate);

        Set<Expression> rewrittenSet = new HashSet<>();
        for (Expression expression : EqualityInference.nonInferrableConjuncts(predicate)) {
            Expression rewritten = inference.rewriteExpression(expression, Predicates.alwaysTrue(), types);
            Preconditions.checkState(rewritten != null, "Rewrite with full variable scope should always be possible");
            rewrittenSet.add(rewritten);
        }
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(Predicates.alwaysTrue(), types).getScopeEqualities());

        return rewrittenSet;
    }

    /**
     * Normalizes Expression nodes (and all sub-expressions) by identity.
     * <p>
     * Identity equality of Expression nodes is necessary for EqualityInference to generate stable rewrites
     * (as specified by Ordering.arbitrary())
     */
    private static class ExpressionIdentityNormalizer
    {
        private final Map<Expression, Expression> expressionCache = new HashMap<>();

        private Expression normalize(Expression expression)
        {
            Expression identityNormalizedExpression = expressionCache.get(expression);
            if (identityNormalizedExpression == null) {
                // Make sure all sub-expressions are normalized first
                for (Expression subExpression : Iterables.filter(SubExpressionExtractor.extract(expression), Predicates.not(Predicates.equalTo(expression)))) {
                    normalize(subExpression);
                }

                // Since we have not seen this expression before, rewrite it entirely in terms of the normalized sub-expressions
                identityNormalizedExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(expressionCache), expression);
                expressionCache.put(identityNormalizedExpression, identityNormalizedExpression);
            }
            return identityNormalizedExpression;
        }
    }
}
