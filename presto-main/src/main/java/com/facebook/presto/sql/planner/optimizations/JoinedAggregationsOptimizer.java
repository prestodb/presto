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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer.HashComputation;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

/**
 * Converts joins with similar subqueries into aggregation
 * SELECT * FROM
 * (
 *      SELECT k, agg1, agg2
 *      FROM t
 *      GROUP BY k
 * ) a
 * JOIN
 * (
 *      SELECT k, agg3, agg4
 *      FROM t
 *      GROUP BY k
 *  ) b
 *  ON (a.k = b.k)
 *
 * as
 *
 * SELECT k, agg1, agg2, agg3, agg4
 * FROM T
 * GROUP BY k
 */
public class JoinedAggregationsOptimizer
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private static class PlanSummary
        {
            private final TableHandle table;
            private final Map<Symbol, ColumnHandle> columnAssignments;
            private final List<Symbol> tableOutputSymbols;
            private final Expression originalConstraint;
            private final Optional<TableLayoutHandle> tableLayoutHandle;
            private final TupleDomain<ColumnHandle> predicate;
            private List<Symbol> groupByKeys;
            private Map<Symbol, Signature> functions;
            private Map<Symbol, FunctionCall> aggregations;
            private Optional<Symbol> hashSymbol;
            private Optional<Map<Symbol, Expression>> projectTableScanAssignment = Optional.empty();
            private Optional<Map<Symbol, Expression>> projectAggregationAssignment = Optional.empty();

            public PlanSummary(TableHandle table,
                                Map<Symbol, ColumnHandle> columnAssignments,
                                List<Symbol> tableOutputSymbols,
                                Expression originalConstraint,
                                Optional<TableLayoutHandle> tableLayoutHandle,
                                TupleDomain<ColumnHandle> predicate)
            {
                this.table = requireNonNull(table, "table is null");
                this.columnAssignments = ImmutableMap.copyOf(requireNonNull(columnAssignments, "columnAssignments is null"));
                this.tableOutputSymbols = ImmutableList.copyOf(requireNonNull(tableOutputSymbols, "tableOutputSymbols is null"));
                this.tableLayoutHandle = requireNonNull(tableLayoutHandle, "tableLayoutHandle is null");
                this.predicate = requireNonNull(predicate, "predicate is null");
                this.originalConstraint = originalConstraint;
            }

            public TableHandle getTable()
            {
                return this.table;
            }

            public Map<Symbol, ColumnHandle> getColumnAssignments()
            {
                return this.columnAssignments;
            }

            public List<Symbol> getTableOutputSymbols()
            {
                return this.tableOutputSymbols;
            }

            public Expression getOriginalConstraint()
            {
                return this.originalConstraint;
            }

            public Optional<TableLayoutHandle> getLayout()
            {
                return this.tableLayoutHandle;
            }

            public TupleDomain<ColumnHandle> getPredicate()
            {
                return this.predicate;
            }

            public void setGroupByKeys(List<Symbol> groupByKeys)
            {
                this.groupByKeys = ImmutableList.copyOf(requireNonNull(groupByKeys, "groupByKeys is null"));
            }

            public List<Symbol> getGroupByKeys()
            {
                return this.groupByKeys;
            }

            public void setFunctions(Map<Symbol, Signature> functions)
            {
                this.functions = ImmutableMap.copyOf(requireNonNull(functions, "functions is null"));
            }

            public Map<Symbol, Signature> getFunctions()
            {
                return this.functions;
            }

            public void setAggregations(Map<Symbol, FunctionCall> aggregations)
            {
                this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
            }

            public Map<Symbol, FunctionCall> getAggregations()
            {
                return this.aggregations;
            }

            public List<Symbol> getAggregationOutputs()
            {
                ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
                symbols.addAll(this.groupByKeys);
                if (this.hashSymbol.isPresent()) {
                    symbols.add(this.hashSymbol.get());
                }
                symbols.addAll(this.aggregations.keySet());
                return symbols.build();
            }

            public Optional<Map<Symbol, Expression>> getProjectTableScanAssignment()
            {
                return this.projectTableScanAssignment;
            }

            public void setProjectTableScanAssignment(Optional<Map<Symbol, Expression>> projectTableScanAssignment)
            {
                this.projectTableScanAssignment = requireNonNull(projectTableScanAssignment, "projectTableScanAssignment is null");
            }

            public Optional<Symbol> getHashSymbol()
            {
                return this.hashSymbol;
            }

            public void setHashSymbol(Optional<Symbol> hashSymbol)
            {
                this.hashSymbol = requireNonNull(hashSymbol, "hashSymbol is null");
            }

            public Optional<Map<Symbol, Expression>> getProjectAggregationAssignment()
            {
                return this.projectAggregationAssignment;
            }

            public void setProjectAggregationAssignment(Optional<Map<Symbol, Expression>> projectAggregationAssignment)
            {
                this.projectAggregationAssignment = requireNonNull(projectAggregationAssignment, "projectAggregationAssignment is null");
            }
        }

        private static class SummaryExtractor
                extends PlanVisitor<Boolean, PlanSummary>
        {
            @Override
            protected PlanSummary visitPlan(PlanNode node, Boolean visitedAggregation)
            {
                return null;
            }

            public static PlanSummary extract(PlanNode node)
            {
                return node.accept(new SummaryExtractor(), Boolean.FALSE);
            }

            @Override
            public PlanSummary visitAggregation(AggregationNode node, Boolean visitedAggregation)
            {
                if (visitedAggregation.booleanValue()) {
                    return null;
                }

                PlanNode source = node.getSource();
                PlanSummary planSummary = null;
                if (source instanceof TableScanNode) {
                    planSummary = ((TableScanNode) source).accept(this, Boolean.TRUE);
                }
                if (source instanceof ProjectNode) {
                    planSummary = ((ProjectNode) source).accept(this, Boolean.TRUE);
                }

                if (planSummary != null) {
                    planSummary.setGroupByKeys(node.getGroupBy());
                    planSummary.setFunctions(node.getFunctions());
                    planSummary.setAggregations(node.getAggregations());
                    planSummary.setHashSymbol(node.getHashSymbol());
                }
                return planSummary;
            }

            @Override
            public PlanSummary visitProject(ProjectNode node, Boolean visitedAggregation)
            {
                PlanNode source = node.getSource();
                PlanSummary planSummary = null;
                if (source instanceof TableScanNode && visitedAggregation.booleanValue()) {
                    planSummary = ((TableScanNode) source).accept(this, Boolean.TRUE);
                    if (planSummary != null) {
                        planSummary.setProjectTableScanAssignment(Optional.of(node.getAssignments()));
                    }
                }

                if (source instanceof AggregationNode && !visitedAggregation.booleanValue()) {
                    planSummary = ((AggregationNode) source).accept(this, Boolean.FALSE);
                    if (planSummary != null) {
                        planSummary.setProjectAggregationAssignment(Optional.of(node.getAssignments()));
                    }
                }
                return planSummary;
            }

            @Override
            public PlanSummary visitTableScan(TableScanNode node, Boolean visitedAggregation)
            {
                if (!visitedAggregation.booleanValue()) {
                    return null;
                }
                return new PlanSummary(node.getTable(),
                                        node.getAssignments(),
                                        node.getOutputSymbols(),
                                        node.getOriginalConstraint(),
                                        node.getLayout(),
                                        node.getCurrentConstraint());
            }
        }

        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (node.getCriteria().isEmpty() ||
                (node.getType() != JoinNode.Type.INNER)) {
                return node;
            }

            // get summary for left and right
            PlanSummary leftSummary = SummaryExtractor.extract(node.getLeft());
            PlanSummary rightSummary = SummaryExtractor.extract(node.getRight());

            // check optimize criteria
            if (!canOptimize(node, leftSummary, rightSummary)) {
                return node;
            }

            // build new optimized plan
            return buildPlan(node, leftSummary, rightSummary);
        }

        // optimize criteria:
        // A. groupby keys are the same as join keys
        // B. scanning the same table
        private boolean canOptimize(JoinNode node, PlanSummary leftSummary, PlanSummary rightSummary)
        {
            if (leftSummary == null || rightSummary == null) {
                return false;
            }

            if (leftSummary.getGroupByKeys().isEmpty() ||
                rightSummary.getGroupByKeys().isEmpty() ||
                leftSummary.getGroupByKeys().size() != rightSummary.getGroupByKeys().size()) {
                return false;
            }

           Set<Symbol> leftJoinSymbols = ImmutableSet.copyOf(Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft));
           Set<Symbol> rightJoinSymbols = ImmutableSet.copyOf(Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight));

            if (!ImmutableSet.copyOf(rightSummary.getGroupByKeys()).equals(rightJoinSymbols) ||
                !ImmutableSet.copyOf(leftSummary.getGroupByKeys()).equals(leftJoinSymbols)) {
                return false;
            }

            return leftSummary.getTable().equals(rightSummary.getTable());
        }

        private PlanNode buildPlan(JoinNode node, PlanSummary leftSummary, PlanSummary rightSummary)
        {
            // left and right have different groupby symbols, which refers to the same table output column
            // build optimized plan with new groupby symbols
            // finally, add a ProjectNode on top of optimized plan to preserve OutputNode's symbol
            List<Symbol> groupByKeyOutputSymbols = new ArrayList<Symbol>();
            Map<Symbol, ColumnHandle> groupByKeyToColumnMap = new HashMap<>();

            // allocate new groupby symbols
            // build old groupby symbol to new groupby symbol map
            Map<Symbol, Expression> leftGroupByKeyAssignments = new HashMap<>();
            Map<Symbol, Expression> rightGroupByKeyAssignments = new HashMap<>();
            Map<ColumnHandle, Symbol> leftColumnToSymbolMap = ImmutableBiMap.copyOf(leftSummary.getColumnAssignments()).inverse();
            for (Symbol groupByKey : rightSummary.getGroupByKeys()) {
                Symbol symbol = symbolAllocator.newSymbol(groupByKey.getName(),
                                                        symbolAllocator.getTypes().get(groupByKey));
                groupByKeyOutputSymbols.add(symbol);
                Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                ColumnHandle columnHandle = rightSummary.getColumnAssignments().get(groupByKey);
                rightGroupByKeyAssignments.put(groupByKey, expression);
                leftGroupByKeyAssignments.put(leftColumnToSymbolMap.get(columnHandle), expression);
                groupByKeyToColumnMap.put(symbol, columnHandle);
            }

            // build new hash symbol
            Optional<Symbol> hashSymbol = Optional.empty();
            Optional<Expression> hashExpression = Optional.empty();
            if (leftSummary.getHashSymbol().isPresent() && rightSummary.getHashSymbol().isPresent()) {
                hashSymbol = Optional.of(symbolAllocator.newHashSymbol());
                HashComputation hashComputation = new HashComputation(groupByKeyOutputSymbols);
                hashExpression = Optional.of(hashComputation.getHashExpression());
            }

            // build new tableScan with new output symbols
            TableScanNode tableScanNode = buildTableScan(leftSummary,
                                                        rightSummary,
                                                        groupByKeyOutputSymbols,
                                                        groupByKeyToColumnMap);

            // build new AggregationNode with new groupby symbols
            AggregationNode aggregationNode = buildAggregation(leftSummary,
                                                                rightSummary,
                                                                groupByKeyOutputSymbols,
                                                                tableScanNode,
                                                                hashSymbol,
                                                                hashExpression);

            // build Project on top of Aggregation
            boolean isLeftHasProjectOnAggregation = leftSummary.getProjectAggregationAssignment().isPresent();
            boolean isRightHasProjectOnAggregation = rightSummary.getProjectAggregationAssignment().isPresent();
            if (isLeftHasProjectOnAggregation || isRightHasProjectOnAggregation) {
                Map<Symbol, Expression> leftAssignment = isLeftHasProjectOnAggregation ?
                                                        leftSummary.getProjectAggregationAssignment().get() :
                                                        buildAssignment(leftSummary.getAggregationOutputs());
                Map<Symbol, Expression> rightAssignment = isRightHasProjectOnAggregation ?
                                                        rightSummary.getProjectAggregationAssignment().get() :
                                                        buildAssignment(rightSummary.getAggregationOutputs());
                Map<Symbol, Expression> assignments = mergeAssignments(leftAssignment,
                                                                        rightAssignment,
                                                                        leftSummary.getHashSymbol(),
                                                                        rightSummary.getHashSymbol(),
                                                                        hashSymbol,
                                                                        hashExpression,
                                                                        leftSummary.getGroupByKeys(),
                                                                        rightSummary.getGroupByKeys(),
                                                                        groupByKeyOutputSymbols);
                // add ProjectNode on top of optimized plan to preserve OutputNode's symbol
                assignments.putAll(leftGroupByKeyAssignments);
                assignments.putAll(rightGroupByKeyAssignments);
                for (Symbol symbol : groupByKeyOutputSymbols) {
                    assignments.remove(symbol);
                }
                return new ProjectNode(idAllocator.getNextId(), aggregationNode, assignments);
            }
            // add ProjectNode on top of optimized plan to preserve OutputNode's symbol
            return buildProject(aggregationNode,
                                leftGroupByKeyAssignments,
                                rightGroupByKeyAssignments,
                                groupByKeyOutputSymbols);
        }

        // merge two assignments into one, update groupby symbols and hash symbol
        private Map<Symbol, Expression> mergeAssignments(Map<Symbol, Expression> leftAssignment,
                                                            Map<Symbol, Expression> rightAssignment,
                                                            Optional<Symbol> leftHashSymbol,
                                                            Optional<Symbol> rightHashSymbol,
                                                            Optional<Symbol> hashSymbol,
                                                            Optional<Expression> hashExpression,
                                                            List<Symbol> leftGroupByKeys,
                                                            List<Symbol> rightGroupByKeys,
                                                            List<Symbol> groupByOutputKeys)
        {
            Map<Symbol, Expression> assignments = new HashMap<>();
            assignments.putAll(leftAssignment);
            assignments.putAll(rightAssignment);
            if (leftHashSymbol.isPresent()) {
                assignments.remove(leftHashSymbol.get());
            }
            if (rightHashSymbol.isPresent()) {
                assignments.remove(rightHashSymbol.get());
            }
            if (hashSymbol.isPresent()) {
                assignments.put(hashSymbol.get(), hashExpression.get());
            }

            for (Symbol leftGroupByKey : leftGroupByKeys) {
                assignments.remove(leftGroupByKey);
            }
            for (Symbol rightGroupByKey : rightGroupByKeys) {
                assignments.remove(rightGroupByKey);
            }

            for (Symbol groupByKey : groupByOutputKeys) {
                Expression expression = new QualifiedNameReference(groupByKey.toQualifiedName());
                assignments.put(groupByKey, expression);
            }
            return assignments;
        }

        private Map<Symbol, Expression> buildAssignment(List<Symbol> outputSymbols)
        {
            ImmutableMap.Builder<Symbol, Expression> assignment = ImmutableMap.builder();
            for (Symbol symbol : outputSymbols) {
                Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                assignment.put(symbol, expression);
            }
            return assignment.build();
        }

        // build Project on top of Aggregation, add old groupby symbol to new groupby symbol map
        private ProjectNode buildProject(AggregationNode aggregationNode,
                                            Map<Symbol, Expression> leftGroupByKeyAssignments,
                                            Map<Symbol, Expression> rightGroupByKeyAssignments,
                                            List<Symbol> groupByKeyOutputSymbols)
        {
            Map<Symbol, Expression> assignments = new HashMap<>();
            assignments.putAll(leftGroupByKeyAssignments);
            assignments.putAll(rightGroupByKeyAssignments);
            for (Symbol symbol : aggregationNode.getOutputSymbols()) {
                assignments.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
            }
            for (Symbol symbol : groupByKeyOutputSymbols) {
                assignments.remove(symbol);
            }
            return new ProjectNode(idAllocator.getNextId(), aggregationNode, assignments);
        }

        // build new tableScan use new output symbols
        private TableScanNode buildTableScan(PlanSummary leftSummary,
                                            PlanSummary rightSummary,
                                            List<Symbol> groupByKeyOutputSymbols,
                                            Map<Symbol, ColumnHandle> groupByKeyToColumnMap)
        {
            List<Symbol> outputSymbols = new ArrayList<>(groupByKeyOutputSymbols);
            Map<Symbol, ColumnHandle> assignments = new HashMap<>(groupByKeyToColumnMap);

            for (Map.Entry<Symbol, ColumnHandle> entry : leftSummary.getColumnAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                if (!leftSummary.getGroupByKeys().contains(symbol)) {
                    assignments.put(symbol, columnHandle);
                    outputSymbols.add(symbol);
                }
            }

            for (Map.Entry<Symbol, ColumnHandle> entry : rightSummary.getColumnAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                if (!rightSummary.getGroupByKeys().contains(symbol)) {
                    assignments.put(symbol, columnHandle);
                    outputSymbols.add(symbol);
                }
            }

            return new TableScanNode(idAllocator.getNextId(),
                                    leftSummary.getTable(),
                                    outputSymbols,
                                    assignments,
                                    leftSummary.getLayout(),
                                    leftSummary.getPredicate(),
                                    leftSummary.getOriginalConstraint());
        }

        // build new Aggregation using summaries
        private AggregationNode buildAggregation(PlanSummary leftSummary,
                                                PlanSummary rightSummary,
                                                List<Symbol> groupByKeys,
                                                TableScanNode tableScanNode,
                                                Optional<Symbol> hashSymbol,
                                                Optional<Expression> hashExpression)
        {
            Map<Symbol, Signature> functions = new HashMap<>();
            functions.putAll(leftSummary.getFunctions());
            functions.putAll(rightSummary.getFunctions());

            Map<Symbol, FunctionCall> aggregations = new HashMap<>();
            aggregations.putAll(leftSummary.getAggregations());
            aggregations.putAll(rightSummary.getAggregations());

            // build Project on top of TableScan
            boolean isLeftHasProjectOnTableScan = leftSummary.getProjectTableScanAssignment().isPresent();
            boolean isRightHasProjectOnTableScan = rightSummary.getProjectTableScanAssignment().isPresent();
            if (isLeftHasProjectOnTableScan || isRightHasProjectOnTableScan || hashSymbol.isPresent()) {
                Map<Symbol, Expression> leftAssignment = isLeftHasProjectOnTableScan ?
                                                        leftSummary.getProjectTableScanAssignment().get() :
                                                        buildAssignment(leftSummary.getTableOutputSymbols());
                Map<Symbol, Expression> rightAssignment = isRightHasProjectOnTableScan ?
                                                        rightSummary.getProjectTableScanAssignment().get() :
                                                        buildAssignment(rightSummary.getTableOutputSymbols());
                Map<Symbol, Expression> assignments = mergeAssignments(leftAssignment,
                                                                        rightAssignment,
                                                                        leftSummary.getHashSymbol(),
                                                                        rightSummary.getHashSymbol(),
                                                                        hashSymbol,
                                                                        hashExpression,
                                                                        leftSummary.getGroupByKeys(),
                                                                        rightSummary.getGroupByKeys(),
                                                                        groupByKeys);
                // build Aggregation on top of Project
                return new AggregationNode(idAllocator.getNextId(),
                                            new ProjectNode(idAllocator.getNextId(), tableScanNode, assignments),
                                            groupByKeys,
                                            aggregations,
                                            functions,
                                            Collections.emptyMap(),
                                            ImmutableList.of(groupByKeys),
                                            SINGLE,
                                            Optional.empty(),
                                            1.0,
                                            hashSymbol);
            }
            // build Aggregation on top of TableScan
            return new AggregationNode(idAllocator.getNextId(),
                                        tableScanNode,
                                        groupByKeys,
                                        aggregations,
                                        functions,
                                        Collections.emptyMap(),
                                        ImmutableList.of(groupByKeys),
                                        SINGLE,
                                        Optional.empty(),
                                        1.0,
                                        Optional.empty());
        }
    }
}
