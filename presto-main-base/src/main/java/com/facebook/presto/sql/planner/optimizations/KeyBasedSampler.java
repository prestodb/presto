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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.Varchars;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getKeyBasedSamplingFunction;
import static com.facebook.presto.SystemSessionProperties.getKeyBasedSamplingPercentage;
import static com.facebook.presto.SystemSessionProperties.isKeyBasedSamplingEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.StandardWarningCode.SAMPLED_FIELDS;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public class KeyBasedSampler
        implements PlanOptimizer
{
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public KeyBasedSampler(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isKeyBasedSamplingEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            List<String> sampledFields = new ArrayList<>(2);
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata.getFunctionAndTypeManager(), idAllocator, sampledFields), plan, null);

            if (!isEnabledForTesting) {
                if (!sampledFields.isEmpty()) {
                    warningCollector.add(new PrestoWarning(SAMPLED_FIELDS, String.format("Sampled the following columns/derived columns at %s percent:%n\t%s", getKeyBasedSamplingPercentage(session) * 100., String.join("\n\t", sampledFields))));
                }
                else {
                    warningCollector.add(new PrestoWarning(SEMANTIC_WARNING, "Sampling could not be performed due to the query structure"));
                }
            }

            return PlanOptimizerResult.optimizerResult(rewritten, true);
        }

        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanNodeIdAllocator idAllocator;
        private final List<String> sampledFields;

        private Rewriter(Session session, FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator idAllocator, List<String> sampledFields)
        {
            this.session = requireNonNull(session, "session is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.sampledFields = requireNonNull(sampledFields, "sampledFields is null");
        }

        private PlanNode addSamplingFilter(PlanNode tableScanNode, Optional<VariableReferenceExpression> rowExpressionOptional, FunctionAndTypeManager functionAndTypeManager)
        {
            if (!rowExpressionOptional.isPresent()) {
                return tableScanNode;
            }
            RowExpression rowExpression = rowExpressionOptional.get();
            RowExpression arg;
            Type type = rowExpression.getType();
            if (!Varchars.isVarcharType(type)) {
                arg = call(
                        "CAST",
                        functionAndTypeManager.lookupCast(CAST, rowExpression.getType(), VARCHAR),
                        VARCHAR,
                        rowExpression);
            }
            else {
                arg = rowExpression;
            }

            RowExpression sampledArg;
            try {
                sampledArg = call(
                        functionAndTypeManager,
                        getKeyBasedSamplingFunction(session),
                        DOUBLE,
                        ImmutableList.of(arg));
            }
            catch (PrestoException prestoException) {
                throw new PrestoException(FUNCTION_NOT_FOUND, String.format("Sampling function: %s not cannot be resolved", getKeyBasedSamplingFunction(session)), prestoException);
            }

            RowExpression predicate = call(
                    LESS_THAN_OR_EQUAL.name(),
                    functionAndTypeManager.resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, fromTypes(DOUBLE, DOUBLE)),
                    BOOLEAN,
                    sampledArg,
                    new ConstantExpression(arg.getSourceLocation(), getKeyBasedSamplingPercentage(session), DOUBLE));

            FilterNode filterNode = new FilterNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScanNode,
                    predicate);

            String tableName;
            while (tableScanNode instanceof FilterNode || tableScanNode instanceof ProjectNode) {
                tableScanNode = tableScanNode.getSources().get(0);
            }
            if (tableScanNode instanceof TableScanNode) {
                tableName = ((TableScanNode) tableScanNode).getTable().getConnectorHandle().toString();
            }
            else {
                tableName = "plan node: " + tableScanNode.getId();
            }

            sampledFields.add(String.format("%s from %s", rowExpression, tableName));
            return filterNode;
        }

        private Optional<VariableReferenceExpression> findSuitableKey(List<VariableReferenceExpression> keys)
        {
            Optional<VariableReferenceExpression> variableReferenceExpression = keys.stream()
                    .filter(x -> TypeUtils.isIntegralType(x.getType().getTypeSignature(), functionAndTypeManager))
                    .findFirst();

            if (!variableReferenceExpression.isPresent()) {
                variableReferenceExpression = keys.stream()
                        .filter(x -> Varchars.isVarcharType(x.getType()))
                        .findFirst();
            }

            return variableReferenceExpression;
        }

        private PlanNode sampleSourceNodeWithKey(PlanNode planNode, PlanNode source, List<VariableReferenceExpression> keys)
        {
            PlanNode rewrittenSource = rewriteWith(this, source);
            if (rewrittenSource == source) {
                // Source not rewritten so we sample here.
                rewrittenSource = addSamplingFilter(source, findSuitableKey(keys), functionAndTypeManager);
            }

            // Always return new
            return replaceChildren(planNode, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();

            PlanNode rewrittenLeft = rewriteWith(this, left);
            PlanNode rewrittenRight = rewriteWith(this, right);

            // If at least one of them is unchanged means it had no join. So one side has a table scan.
            // So we apply filter on both sides.
            if (left == rewrittenLeft || right == rewrittenRight) {
                // Sample both sides if at least one side is not already sampled

                // Find the best equijoin clause so we sample both sides the same way optimally
                // First see if there is a int/bigint key
                Optional<EquiJoinClause> equiJoinClause = node.getCriteria().stream()
                        .filter(x -> TypeUtils.isIntegralType(x.getLeft().getType().getTypeSignature(), functionAndTypeManager))
                        .findFirst();
                if (!equiJoinClause.isPresent()) {
                    // See if there is a varchar key
                    equiJoinClause = node.getCriteria().stream()
                            .filter(x -> Varchars.isVarcharType(x.getLeft().getType()))
                            .findFirst();
                }

                if (equiJoinClause.isPresent()) {
                    rewrittenLeft = addSamplingFilter(rewrittenLeft, Optional.of(equiJoinClause.get().getLeft()), functionAndTypeManager);
                    rewrittenRight = addSamplingFilter(rewrittenRight, Optional.of(equiJoinClause.get().getRight()), functionAndTypeManager);
                }
            }

            // We always return new from join so others won't be applied if not needed.
            return replaceChildren(node, ImmutableList.of(rewrittenLeft, rewrittenRight));
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode source = node.getSource();
            PlanNode filteringSource = node.getFilteringSource();
            PlanNode rewrittenSource = rewriteWith(this, source);
            PlanNode rewrittenFilteringSource = rewriteWith(this, filteringSource);
            if (rewrittenSource == source || rewrittenFilteringSource == filteringSource) {
                rewrittenSource = addSamplingFilter(rewrittenSource, findSuitableKey(ImmutableList.of(node.getSourceJoinVariable())), functionAndTypeManager);
                rewrittenFilteringSource = addSamplingFilter(rewrittenFilteringSource, findSuitableKey(ImmutableList.of(node.getFilteringSourceJoinVariable())), functionAndTypeManager);
            }

            return replaceChildren(node, ImmutableList.of(rewrittenSource, rewrittenFilteringSource));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            return sampleSourceNodeWithKey(node, node.getSource(), node.getGroupingKeys());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            return sampleSourceNodeWithKey(node, node.getSource(), node.getPartitionBy());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Void> context)
        {
            return sampleSourceNodeWithKey(node, node.getSource(), node.getPartitionBy());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Void> context)
        {
            return sampleSourceNodeWithKey(node, node.getSource(), node.getPartitionBy());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Void> context)
        {
            return sampleSourceNodeWithKey(node, node.getSource(), node.getDistinctVariables());
        }
    }
}
