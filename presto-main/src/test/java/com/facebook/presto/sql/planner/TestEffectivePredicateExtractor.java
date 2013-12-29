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

import com.facebook.presto.connector.dual.DualColumnHandle;
import com.facebook.presto.connector.dual.DualTableHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.connector.dual.DualSplitManager.DualPartition;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;

public class TestEffectivePredicateExtractor
{
    private static final Symbol A = new Symbol("a");
    private static final Symbol B = new Symbol("b");
    private static final Symbol C = new Symbol("c");
    private static final Symbol D = new Symbol("d");
    private static final Symbol E = new Symbol("e");
    private static final Symbol F = new Symbol("f");
    private static final Expression AE = symbolExpr(A);
    private static final Expression BE = symbolExpr(B);
    private static final Expression CE = symbolExpr(C);
    private static final Expression DE = symbolExpr(D);
    private static final Expression EE = symbolExpr(E);
    private static final Expression FE = symbolExpr(F);

    private Map<Symbol, ColumnHandle> scanAssignments;
    private TableScanNode baseTableScan;
    private ExpressionIdentityNormalizer expressionNormalizer;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        scanAssignments = ImmutableMap.<Symbol, ColumnHandle>builder()
                .put(A, new DualColumnHandle("a"))
                .put(B, new DualColumnHandle("b"))
                .put(C, new DualColumnHandle("c"))
                .put(D, new DualColumnHandle("d"))
                .put(E, new DualColumnHandle("e"))
                .put(F, new DualColumnHandle("f"))
                .build();

        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D, E, F)));
        baseTableScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        expressionNormalizer = new ExpressionIdentityNormalizer();
    }

    @Test
    public void testAggregation()
            throws Exception
    {
        PlanNode node = new AggregationNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, DE),
                                equals(BE, EE),
                                equals(CE, FE),
                                lessThan(DE, number(10)),
                                lessThan(CE, DE),
                                greaterThan(AE, number(2)),
                                equals(EE, FE))),
                ImmutableList.of(A, B, C),
                ImmutableMap.of(C, fakeFunction("test"), D, fakeFunction("test")),
                ImmutableMap.of(C, fakeFunctionHandle("test"), D, fakeFunctionHandle("test")),
                AggregationNode.Step.FINAL);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Rewrite in terms of group by symbols
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(AE, number(10)),
                        lessThan(BE, AE),
                        greaterThan(AE, number(2)),
                        equals(BE, CE)));
    }

    @Test
    public void testFilter()
            throws Exception
    {
        PlanNode node = filter(baseTableScan,
                and(
                        greaterThan(AE, new FunctionCall(QualifiedName.of("rand"), ImmutableList.<Expression>of())),
                        lessThan(BE, number(10))));

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Non-deterministic functions should be purged
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, number(10))));
    }

    @Test
    public void testProject()
            throws Exception
    {
        PlanNode node = new ProjectNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, number(10)))),
                ImmutableMap.of(D, AE, E, CE));

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Rewrite in terms of project output symbols
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        lessThan(DE, number(10)),
                        equals(DE, EE)));
    }

    @Test
    public void testTopN()
            throws Exception
    {
        PlanNode node = new TopNNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, number(10)))),
                1, ImmutableList.of(A), ImmutableMap.of(A, SortItem.Ordering.ASCENDING), true);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Pass through
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, number(10))));
    }

    @Test
    public void testLimit()
            throws Exception
    {
        PlanNode node = new LimitNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, number(10)))),
                1);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Pass through
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, number(10))));
    }

    @Test
    public void testSort()
            throws Exception
    {
        PlanNode node = new SortNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, number(10)))),
                ImmutableList.of(A), ImmutableMap.of(A, SortItem.Ordering.ASCENDING));

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Pass through
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, number(10))));
    }

    @Test
    public void testWindow()
            throws Exception
    {
        PlanNode node = new WindowNode(newId(),
                filter(baseTableScan,
                        and(
                                equals(AE, BE),
                                equals(BE, CE),
                                lessThan(CE, number(10)))),
                ImmutableList.of(A),
                ImmutableList.of(A),
                ImmutableMap.of(A, SortItem.Ordering.ASCENDING),
                ImmutableMap.<Symbol, FunctionCall>of(),
                ImmutableMap.<Symbol, FunctionHandle>of());

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Pass through
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(
                        equals(AE, BE),
                        equals(BE, CE),
                        lessThan(CE, number(10))));
    }

    @Test
    public void testTableScan()
            throws Exception
    {
        // Effective predicate is True if there are no generated partitions
        Map<Symbol, ColumnHandle> assignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C, D)));
        PlanNode node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>absent());
        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

        // tupleDomainInput with no matching partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(scanAssignments.get(A), Domain.singleValue(1L))),
                        ImmutableList.<Partition>of())));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(effectivePredicate, BooleanLiteral.FALSE_LITERAL);

        // tupleDomainInput with non-descriptive partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(scanAssignments.get(A), Domain.singleValue(1L))),
                        ImmutableList.<Partition>of(new DualPartition()))));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(number(1L), AE)));

        // tupleDomainInput with descriptive partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(scanAssignments.get(A), Domain.singleValue(1L))),
                        ImmutableList.<Partition>of(tupleDomainPartition(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(
                                scanAssignments.get(A), Domain.singleValue(1L),
                                scanAssignments.get(B), Domain.singleValue(2L))))))));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(number(2L), BE), equals(number(1L), AE)));

        // generic tupleDomainInput with no matching partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.all(),
                        ImmutableList.<Partition>of())));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(effectivePredicate, BooleanLiteral.FALSE_LITERAL);

        // generic tupleDomainInput with non-descriptive partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.all(),
                        ImmutableList.<Partition>of(new DualPartition()))));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(effectivePredicate, BooleanLiteral.TRUE_LITERAL);

        // generic tupleDomainInput with descriptive partitions
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.all(),
                        ImmutableList.<Partition>of(tupleDomainPartition(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(
                                scanAssignments.get(A), Domain.singleValue(1L),
                                scanAssignments.get(B), Domain.singleValue(2L))))))));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(number(2L), BE), equals(number(1L), AE)));

        // Make sure only output symbols are produced
        node = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.of(A),
                assignments,
                null,
                Optional.<GeneratedPartitions>of(new GeneratedPartitions(
                        TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(
                                scanAssignments.get(A), Domain.singleValue(1L),
                                scanAssignments.get(D), Domain.singleValue(3L))),
                        ImmutableList.<Partition>of(tupleDomainPartition(TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(
                                scanAssignments.get(A), Domain.singleValue(1L),
                                scanAssignments.get(C), Domain.singleValue(2L))))))));
        effectivePredicate = EffectivePredicateExtractor.extract(node);
        Assert.assertEquals(normalizeConjuncts(effectivePredicate), normalizeConjuncts(equals(number(1L), AE)));
    }

    private static Partition tupleDomainPartition(final TupleDomain tupleDomain)
    {
        return new Partition()
        {
            @Override
            public String getPartitionId()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public TupleDomain getTupleDomain()
            {
                return tupleDomain;
            }
        };
    }

    @Test
    public void testUnion()
            throws Exception
    {
        PlanNode node = new UnionNode(newId(),
                ImmutableList.<PlanNode>of(
                        filter(baseTableScan, greaterThan(AE, number(10))),
                        filter(baseTableScan, and(greaterThan(AE, number(10)), lessThan(AE, number(100)))),
                        filter(baseTableScan, and(greaterThan(AE, number(10)), lessThan(AE, number(100))))
                ),
                ImmutableListMultimap.of(A, B, A, C, A, E)
        );

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Only the common conjuncts can be inferred through a Union
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(greaterThan(AE, number(10))));
    }

    @Test
    public void testInnerJoin()
            throws Exception
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(leftAssignments.keySet()),
                leftAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(rightAssignments.keySet()),
                rightAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.INNER,
                filter(leftScan,
                        and(
                                lessThan(BE, AE),
                                lessThan(CE, number(10)))),
                filter(rightScan,
                        and(
                                equals(DE, EE),
                                lessThan(FE, number(100)))),
                criteria);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // All predicates should be carried through
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, number(10)),
                        equals(DE, EE),
                        lessThan(FE, number(100)),
                        equals(AE, DE),
                        equals(BE, EE)));
    }

    @Test
    public void testLeftJoin()
            throws Exception
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(leftAssignments.keySet()),
                leftAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(rightAssignments.keySet()),
                rightAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.LEFT,
                filter(leftScan,
                        and(
                                lessThan(BE, AE),
                                lessThan(CE, number(10)))),
                filter(rightScan,
                        and(
                                equals(DE, EE),
                                lessThan(FE, number(100)))),
                criteria);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // All right side symbols should be checked against NULL
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(lessThan(BE, AE),
                        lessThan(CE, number(10)),
                        or(equals(DE, EE), and(isNull(DE), isNull(EE))),
                        or(lessThan(FE, number(100)), isNull(FE)),
                        or(equals(AE, DE), isNull(DE)),
                        or(equals(BE, EE), isNull(EE))));
    }

    @Test
    public void testRightJoin()
            throws Exception
    {
        ImmutableList.Builder<JoinNode.EquiJoinClause> criteriaBuilder = ImmutableList.builder();
        criteriaBuilder.add(new JoinNode.EquiJoinClause(A, D));
        criteriaBuilder.add(new JoinNode.EquiJoinClause(B, E));
        List<JoinNode.EquiJoinClause> criteria = criteriaBuilder.build();

        Map<Symbol, ColumnHandle> leftAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(A, B, C)));
        TableScanNode leftScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(leftAssignments.keySet()),
                leftAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        Map<Symbol, ColumnHandle> rightAssignments = Maps.filterKeys(scanAssignments, Predicates.in(ImmutableList.of(D, E, F)));
        TableScanNode rightScan = new TableScanNode(
                newId(),
                new DualTableHandle("default"),
                ImmutableList.copyOf(rightAssignments.keySet()),
                rightAssignments,
                null,
                Optional.<GeneratedPartitions>absent()
        );

        PlanNode node = new JoinNode(newId(),
                JoinNode.Type.RIGHT,
                filter(leftScan,
                        and(
                                lessThan(BE, AE),
                                lessThan(CE, number(10)))),
                filter(rightScan,
                        and(
                                equals(DE, EE),
                                lessThan(FE, number(100)))),
                criteria);

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // All left side symbols should be checked against NULL
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(or(lessThan(BE, AE), and(isNull(BE), isNull(AE))),
                        or(lessThan(CE, number(10)), isNull(CE)),
                        equals(DE, EE),
                        lessThan(FE, number(100)),
                        or(equals(AE, DE), isNull(AE)),
                        or(equals(BE, EE), isNull(BE))));
    }

    @Test
    public void testSemiJoin()
            throws Exception
    {
        PlanNode node = new SemiJoinNode(newId(),
                filter(baseTableScan, and(greaterThan(AE, number(10)), lessThan(AE, number(100)))),
                filter(baseTableScan, greaterThan(AE, number(5))),
                A, B, C
        );

        Expression effectivePredicate = EffectivePredicateExtractor.extract(node);

        // Currently, only pull predicates through the source plan
        Assert.assertEquals(normalizeConjuncts(effectivePredicate),
                normalizeConjuncts(and(greaterThan(AE, number(10)), lessThan(AE, number(100)))));
    }

    private static PlanNodeId newId()
    {
        return new PlanNodeId(UUID.randomUUID().toString());
    }

    private static FilterNode filter(PlanNode source, Expression predicate)
    {
        return new FilterNode(newId(), source, predicate);
    }

    private static Expression symbolExpr(Symbol symbol)
    {
        return new QualifiedNameReference(symbol.toQualifiedName());
    }

    private static Expression number(long number)
    {
        return new LongLiteral(String.valueOf(number));
    }

    private static ComparisonExpression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Type.EQUAL, expression1, expression2);
    }

    private static ComparisonExpression lessThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Type.LESS_THAN, expression1, expression2);
    }

    private static ComparisonExpression greaterThan(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(ComparisonExpression.Type.GREATER_THAN, expression1, expression2);
    }

    private static IsNullPredicate isNull(Expression expression)
    {
        return new IsNullPredicate(expression);
    }

    private static FunctionCall fakeFunction(String name)
    {
        return new FunctionCall(QualifiedName.of("test"), ImmutableList.<Expression>of());
    }

    private static FunctionHandle fakeFunctionHandle(String name)
    {
        return new FunctionHandle(Math.abs(new Random().nextInt()), name);
    }

    private Set<Expression> normalizeConjuncts(Expression... conjuncts)
    {
        return normalizeConjuncts(Arrays.asList(conjuncts));
    }

    private Set<Expression> normalizeConjuncts(Iterable<Expression> conjuncts)
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
            Expression rewritten = inference.rewriteExpression(expression, Predicates.<Symbol>alwaysTrue());
            Preconditions.checkState(rewritten != null, "Rewrite with full symbol scope should always be possible");
            rewrittenSet.add(rewritten);
        }
        rewrittenSet.addAll(inference.generateEqualitiesPartitionedBy(Predicates.<Symbol>alwaysTrue()).getScopeEqualities());

        return rewrittenSet;
    }

    /**
     * Normalizes Expression nodes (and all sub-expressions) by identity.
     * <p/>
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
