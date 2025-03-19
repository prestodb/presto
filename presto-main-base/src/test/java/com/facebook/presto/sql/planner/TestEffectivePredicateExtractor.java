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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.EqualityInference.Builder.nonInferableConjuncts;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.count;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static org.testng.Assert.assertEquals;

public class TestEffectivePredicateExtractor
{
    private static final TableHandle DUAL_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new TestingMetadata.TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.empty());

    private static final TableHandle DUAL_TABLE_HANDLE_WITH_LAYOUT = new TableHandle(
            new ConnectorId("test"),
            new TestingMetadata.TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));

    private static final VariableReferenceExpression AV = new VariableReferenceExpression(Optional.empty(), "a", BIGINT);
    private static final VariableReferenceExpression BV = new VariableReferenceExpression(Optional.empty(), "b", BIGINT);
    private static final VariableReferenceExpression CV = new VariableReferenceExpression(Optional.empty(), "c", BIGINT);
    private static final VariableReferenceExpression DV = new VariableReferenceExpression(Optional.empty(), "d", BIGINT);
    private static final VariableReferenceExpression EV = new VariableReferenceExpression(Optional.empty(), "e", BIGINT);
    private static final VariableReferenceExpression FV = new VariableReferenceExpression(Optional.empty(), "f", BIGINT);
    private static final VariableReferenceExpression GV = new VariableReferenceExpression(Optional.empty(), "g", BIGINT);

    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
            new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
            new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
            metadata.getFunctionAndTypeManager());
    private final EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(
            new RowExpressionDomainTranslator(metadata),
            metadata.getFunctionAndTypeManager());

    private Map<VariableReferenceExpression, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;

    @BeforeClass
    public void setUp()
    {
        scanAssignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                .put(AV, new TestingMetadata.TestingColumnHandle("a"))
                .put(BV, new TestingMetadata.TestingColumnHandle("b"))
                .put(CV, new TestingMetadata.TestingColumnHandle("c"))
                .put(DV, new TestingMetadata.TestingColumnHandle("d"))
                .put(EV, new TestingMetadata.TestingColumnHandle("e"))
                .put(FV, new TestingMetadata.TestingColumnHandle("f"))
                .build();

        Map<VariableReferenceExpression, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV, DV, EV, FV)));
        baseTableScan = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty());
    }

    @Test
    public void testAggregation()
    {
        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, DV),
                                equals(BV, EV),
                                equals(CV, FV),
                                lessThan(DV, bigintLiteral(10)),
                                lessThan(CV, DV),
                                greaterThan(AV, bigintLiteral(2)),
                                equals(EV, FV))),
                ImmutableMap.of(
                        CV, count(metadata.getFunctionAndTypeManager()),
                        DV, count(metadata.getFunctionAndTypeManager())),
                singleGroupingSet(ImmutableList.of(AV, BV, CV)),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Rewrite in terms of group by symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(AV, bigintLiteral(10)),
                        lessThan(BV, AV),
                        greaterThan(AV, bigintLiteral(2)),
                        equals(BV, CV)));
    }

    @Test
    public void testGroupByEmpty()
    {
        PlanNode node = new AggregationNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan, FALSE_CONSTANT),
                ImmutableMap.of(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.FINAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        assertEquals(effectivePredicate, TRUE_CONSTANT);
    }

    @Test
    public void testFilter()
    {
        PlanNode node = filter(baseTableScan,
                and(
                        greaterThan(AV, call(metadata.getFunctionAndTypeManager(), "rand", DOUBLE, ImmutableList.of())),
                        lessThan(BV, bigintLiteral(10))));

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Non-deterministic functions should be purged
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BV, bigintLiteral(10))));
    }

    @Test
    public void testProject()
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, BV),
                                equals(BV, CV),
                                lessThan(CV, bigintLiteral(10)))),
                assignment(DV, AV, EV, CV));

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Rewrite in terms of project output symbols
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(DV, bigintLiteral(10)),
                        equals(DV, EV)));
    }

    @Test
    public void testProjectOverFilterWithNoReferencedAssignments()
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(call("mod",
                                        metadata.getFunctionAndTypeManager().lookupFunction("mod", fromTypes(BIGINT, BIGINT)),
                                        BIGINT,
                                        ImmutableList.of(CV, bigintLiteral(5L))), bigintLiteral(-1L)),
                                equals(CV, bigintLiteral(10L)))),
                assignment(DV, AV));

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // The filter predicate is reduced to `CV = 10 AND mod(10,5) = -1`
        // Since we have no references to `CV` in the assignments however, neither of these conjuncts is pulled up through the Project
        assertEquals(effectivePredicate, TRUE_CONSTANT);
    }

    @Test
    public void testTopN()
    {
        PlanNode node = new TopNNode(

                Optional.empty(),
                newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, BV),
                                equals(BV, CV),
                                lessThan(CV, bigintLiteral(10)))),
                1, new OrderingScheme(ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_FIRST))), TopNNode.Step.PARTIAL);

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AV, BV),
                        equals(BV, CV),
                        lessThan(CV, bigintLiteral(10))));
    }

    @Test
    public void testLimit()
    {
        PlanNode node = new LimitNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, BV),
                                equals(BV, CV),
                                lessThan(CV, bigintLiteral(10)))),
                1,
                FINAL);

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AV, BV),
                        equals(BV, CV),
                        lessThan(CV, bigintLiteral(10))));
    }

    @Test
    public void testSort()
    {
        PlanNode node = new SortNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, BV),
                                equals(BV, CV),
                                lessThan(CV, bigintLiteral(10)))),
                new OrderingScheme(ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_LAST))),
                false,
                ImmutableList.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AV, BV),
                        equals(BV, CV),
                        lessThan(CV, bigintLiteral(10))));
    }

    @Test
    public void testWindow()
    {
        PlanNode node = new WindowNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan,
                        and(
                                equals(AV, BV),
                                equals(BV, CV),
                                lessThan(CV, bigintLiteral(10)))),
                new DataOrganizationSpecification(
                        ImmutableList.of(AV),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(new Ordering(AV, SortOrder.ASC_NULLS_LAST))))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Pass through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AV, BV),
                        equals(BV, CV),
                        lessThan(CV, bigintLiteral(10))));
    }

    @Test
    public void testTableScan()
    {
        // Effective predicate is True if there is no effective predicate
        Map<VariableReferenceExpression, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV, DV)));
        PlanNode node = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty());
        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);
        assertEquals(effectivePredicate, TRUE_CONSTANT);

        node = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.none(),
                TupleDomain.all(), Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(node);
        assertEquals(effectivePredicate, FALSE_CONSTANT);

        node = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(scanAssignments.get(AV), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all(), Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(node);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(1L), AV)));

        node = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        scanAssignments.get(AV), Domain.singleValue(BIGINT, 1L),
                        scanAssignments.get(BV), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all(), Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(node);
        assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(bigintLiteral(2L), BV), equals(bigintLiteral(1L), AV)));

        node = new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty());
        effectivePredicate = effectivePredicateExtractor.extract(node);
        assertEquals(effectivePredicate, TRUE_CONSTANT);
    }

    @Test
    public void testUnion()
    {
        PlanNode node = new UnionNode(
                Optional.empty(),
                newId(),
                ImmutableList.of(
                        filter(baseTableScan, greaterThan(AV, bigintLiteral(10))),
                        filter(baseTableScan, and(greaterThan(AV, bigintLiteral(10)), lessThan(AV, bigintLiteral(100)))),
                        filter(baseTableScan, and(greaterThan(AV, bigintLiteral(10)), lessThan(AV, bigintLiteral(100))))),
                ImmutableList.of(AV),
                ImmutableMap.of(AV, ImmutableList.of(BV, CV, EV)));

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Only the common conjuncts can be inferred through a Union
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(greaterThan(AV, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoin()
    {
        ImmutableList.Builder<EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new EquiJoinClause(AV, DV));
        criteriaBuilder.add(new EquiJoinClause(BV, EV));
        List<EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        equals(GV, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100))));

        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.INNER,
                left,
                right,
                criteria,
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.of(lessThanOrEqual(BV, EV)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // All predicates having output symbol should be carried through
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100)),
                        equals(AV, DV),
                        equals(BV, EV),
                        lessThanOrEqual(BV, EV)));
    }

    @Test
    public void testInnerJoinPropagatesPredicatesViaEquiConditions()
    {
        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, equals(AV, bigintLiteral(10)));

        // predicates on "a" column should be propagated to output symbols via join equi conditions
        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.INNER,
                left,
                rightScan,
                ImmutableList.of(new EquiJoinClause(AV, DV)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(rightScan.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        assertEquals(
                normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DV, bigintLiteral(10))));
    }

    @Test
    public void testInnerJoinWithFalseFilter()
    {
        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.INNER,
                leftScan,
                rightScan,
                ImmutableList.of(new EquiJoinClause(AV, DV)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftScan.getOutputVariables())
                        .addAll(rightScan.getOutputVariables())
                        .build(),
                Optional.of(FALSE_CONSTANT),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        assertEquals(effectivePredicate, FALSE_CONSTANT);
    }

    @Test
    public void testLeftJoin()
    {
        ImmutableList.Builder<EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new EquiJoinClause(AV, DV));
        criteriaBuilder.add(new EquiJoinClause(BV, EV));
        List<EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        equals(GV, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100))));
        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.LEFT,
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
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // All right side symbols having output symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        or(equals(DV, EV), and(isNull(DV), isNull(EV))),
                        or(lessThan(FV, bigintLiteral(100)), isNull(FV)),
                        or(equals(AV, DV), isNull(DV)),
                        or(equals(BV, EV), isNull(EV))));
    }

    @Test
    public void testLeftJoinWithFalseInner()
    {
        List<EquiJoinClause> criteria = ImmutableList.of(new EquiJoinClause(AV, DV));

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        equals(GV, bigintLiteral(10))));
        FilterNode right = filter(rightScan, FALSE_CONSTANT);
        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.LEFT,
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
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // False literal on the right side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        or(equals(AV, DV), isNull(DV))));
    }

    @Test
    public void testRightJoin()
    {
        ImmutableList.Builder<EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new EquiJoinClause(AV, DV));
        criteriaBuilder.add(new EquiJoinClause(BV, EV));
        List<EquiJoinClause> criteria = criteriaBuilder.build();

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan,
                and(
                        lessThan(BV, AV),
                        lessThan(CV, bigintLiteral(10)),
                        equals(GV, bigintLiteral(10))));
        FilterNode right = filter(rightScan,
                and(
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100))));
        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.RIGHT,
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
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // All left side symbols should be checked against NULL
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(or(lessThan(BV, AV), and(isNull(BV), isNull(AV))),
                        or(lessThan(CV, bigintLiteral(10)), isNull(CV)),
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100)),
                        or(equals(AV, DV), isNull(AV)),
                        or(equals(BV, EV), isNull(BV))));
    }

    @Test
    public void testRightJoinWithFalseInner()
    {
        List<EquiJoinClause> criteria = ImmutableList.of(new EquiJoinClause(AV, DV));

        Map<VariableReferenceExpression, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(AV, BV, CV)));
        TableScanNode leftScan = tableScanNode(leftAssignments);

        Map<VariableReferenceExpression, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(DV, EV, FV)));
        TableScanNode rightScan = tableScanNode(rightAssignments);

        FilterNode left = filter(leftScan, FALSE_CONSTANT);
        FilterNode right = filter(rightScan,
                and(
                        equals(DV, EV),
                        lessThan(FV, bigintLiteral(100))));
        PlanNode node = new JoinNode(
                Optional.empty(),
                newId(),
                JoinType.RIGHT,
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
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // False literal on the left side should be ignored
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(equals(DV, EV),
                        lessThan(FV, bigintLiteral(100)),
                        or(equals(AV, DV), isNull(AV))));
    }

    @Test
    public void testSemiJoin()
    {
        PlanNode node = new SemiJoinNode(
                Optional.empty(),
                newId(),
                filter(baseTableScan, and(greaterThan(AV, bigintLiteral(10)), lessThan(AV, bigintLiteral(100)))),
                filter(baseTableScan, greaterThan(AV, bigintLiteral(5))),
                AV, BV, CV,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        RowExpression effectivePredicate = effectivePredicateExtractor.extract(node);

        // Currently, only pull predicates through the source plan
        assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(and(greaterThan(AV, bigintLiteral(10)), lessThan(AV, bigintLiteral(100)))));
    }

    private static TableScanNode tableScanNode(Map<VariableReferenceExpression, ColumnHandle> scanAssignments)
    {
        return new TableScanNode(
                Optional.empty(),
                newId(),
                DUAL_TABLE_HANDLE,
                ImmutableList.copyOf(scanAssignments.keySet()),
                scanAssignments,
                TupleDomain.all(),
                TupleDomain.all(), Optional.empty());
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return new FilterNode(Optional.empty(), newId(), source, predicate);
    }

    private static RowExpression bigintLiteral(long number)
    {
        return constant(number, BIGINT);
    }

    private RowExpression equals(RowExpression expression1, RowExpression expression2)
    {
        return compare(EQUAL, expression1, expression2);
    }

    private RowExpression lessThan(RowExpression expression1, RowExpression expression2)
    {
        return compare(LESS_THAN, expression1, expression2);
    }

    private RowExpression lessThanOrEqual(RowExpression expression1, RowExpression expression2)
    {
        return compare(LESS_THAN_OR_EQUAL, expression1, expression2);
    }

    private RowExpression greaterThan(RowExpression expression1, RowExpression expression2)
    {
        return compare(GREATER_THAN, expression1, expression2);
    }

    private RowExpression compare(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getObjectName(),
                metadata.getFunctionAndTypeManager().resolveOperator(type, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private static RowExpression isNull(RowExpression expression)
    {
        return specialForm(IS_NULL, BOOLEAN, expression);
    }

    private Set<RowExpression> normalizeConjuncts(RowExpression... conjuncts)
    {
        return normalizeConjuncts(Arrays.asList(conjuncts));
    }

    private Set<RowExpression> normalizeConjuncts(Collection<RowExpression> conjuncts)
    {
        return normalizeConjuncts(logicalRowExpressions.combineConjuncts(conjuncts));
    }

    private Set<RowExpression> normalizeConjuncts(RowExpression predicate)
    {
        // Normalize the predicate by identity so that the EqualityInference will produce stable rewrites in this test
        // and thereby produce comparable Sets of conjuncts from this method.

        // Equality inference rewrites and equality generation will always be stable across multiple runs in the same JVM
        EqualityInference inference = EqualityInference.createEqualityInference(metadata, predicate);

        Set<RowExpression> rewrittenSet = new HashSet<>();
        for (RowExpression expression : nonInferableConjuncts(metadata, predicate)) {
            RowExpression rewritten = inference.rewriteExpression(expression, Predicates.alwaysTrue());
            Preconditions.checkState(rewritten != null, "Rewrite with full symbol scope should always be possible");
            rewrittenSet.add(rewritten);
        }
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(Predicates.alwaysTrue()).getScopeEqualities());

        return rewrittenSet;
    }
}
