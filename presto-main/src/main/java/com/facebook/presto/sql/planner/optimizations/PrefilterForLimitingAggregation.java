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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.tree.Join;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.PlannerUtils.addAggregation;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.createMapType;
import static com.facebook.presto.sql.planner.PlannerUtils.getHashExpression;
import static com.facebook.presto.sql.planner.PlannerUtils.getTableScanNodeWithOnlyFilterAndProject;
import static com.facebook.presto.sql.planner.PlannerUtils.projectExpressions;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.isAllLowCardinalityGroupByKeys;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.typeConvert;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static java.lang.Boolean.TRUE;

/**
 * An optimization for quicker execution of simple group by + limit queres. In SQL terms, it will be:
 *
 * Original:
 *
 * SELECT SUM(x), userid FROM Table GROUP BY userid LIMIT 1000
 *
 * Rewritten:
 *
 * SELECT SUM(x) , userid FROM Table
 * CROSS JOIN (SELECT MAP_AGG(hash(userid)) m FROM (SELECT DISTINCT userid FROM Table LIMIT 1000)))
 * WHERE IF(CARDINALITY(m)=1000, m[hash(userid)], TRUE)
 *
 * In addition we also add a timeout to the distinctlimit we add so that we don't get stuck trying to find the keys
 */

public class PrefilterForLimitingAggregation
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    public PrefilterForLimitingAggregation(Metadata metadata, StatsCalculator statsCalculator)
    {
        this.metadata = metadata;
        this.statsCalculator = statsCalculator;
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        if (SystemSessionProperties.isPrefilterForGroupbyLimit(session)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata, types, statsCalculator, idAllocator, variableAllocator), plan);
        }

        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final TypeProvider types;
        private final StatsCalculator statsCalculator;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;

        private Rewriter(
                Session session,
                Metadata metadata,
                TypeProvider types,
                StatsCalculator statsCalculator,
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator)
        {
            this.session = session;
            this.metadata = metadata;
            this.types = types;
            this.statsCalculator = statsCalculator;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
        }

        @Override
        public PlanNode visitSort(SortNode sortNode, RewriteContext<Void> context)
        {
            return sortNode;
        }

        @Override
        public PlanNode visitLimit(LimitNode limitNode, RewriteContext<Void> context)
        {
            PlanNode source = rewriteWith(this, limitNode.getSource());
            AggregationNode aggregationNode = null;

            if (source instanceof ProjectNode && ((ProjectNode) source).getSource() instanceof AggregationNode) {
                aggregationNode = (AggregationNode) ((ProjectNode) source).getSource();
            }
            else if (source instanceof AggregationNode) {
                aggregationNode = (AggregationNode) source;
            }

            if (aggregationNode != null &&
                    !aggregationNode.getGroupingKeys().isEmpty()) {
                Optional<TableScanNode> scanNode = getTableScanNodeWithOnlyFilterAndProject(aggregationNode.getSource());
                // Since we duplicate the source of the aggregation - we want to restrict it to simple scan/filter/project
                // so we can do this opportunistic optimization without too much latency/cpu overhead to support common BI usecases
                if (scanNode.isPresent() &&
                        !isAllLowCardinalityGroupByKeys(aggregationNode, scanNode.get(), session, statsCalculator, types, limitNode.getCount())) {
                    PlanNode rewrittenAggregation = addPrefilter(aggregationNode, limitNode.getCount());
                    if (rewrittenAggregation != aggregationNode) {
                        if (source == aggregationNode) {
                            return replaceChildren(limitNode, ImmutableList.of(rewrittenAggregation));
                        }

                        return replaceChildren(limitNode, ImmutableList.of(replaceChildren(source, ImmutableList.of(rewrittenAggregation))));
                    }
                }
            }

            if (source == limitNode.getSource()) {
                return replaceChildren(limitNode, ImmutableList.of(source));
            }

            return limitNode;
        }

        private PlanNode addPrefilter(AggregationNode aggregationNode, long count)
        {
            List<VariableReferenceExpression> keys = aggregationNode.getGroupingKeys().stream().collect(Collectors.toList());
            if (keys.isEmpty()) {
                return aggregationNode;
            }

            PlanNode originalSource = aggregationNode.getSource();
            PlanNode keySource = clonePlanNode(originalSource, session, metadata, idAllocator, keys, new HashMap<>());
            // TODO(kaikalur): See if timetout can be done in a cleaner way in the middle tier
            DistinctLimitNode timedDistinctLimitNode = new DistinctLimitNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    keySource,
                    count,
                    false,
                    keys,
                    Optional.empty(),
                    SystemSessionProperties.getPrefilterForGroupbyLimitTimeoutMS(session));

            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            RowExpression leftHashExpression = getHashExpression(functionAndTypeManager, keys).get();
            RowExpression rightHashExpression = getHashExpression(functionAndTypeManager, timedDistinctLimitNode.getOutputVariables()).get();

            Type mapType = createMapType(functionAndTypeManager, BIGINT, BOOLEAN);
            PlanNode rightProjectNode = projectExpressions(timedDistinctLimitNode, idAllocator, variableAllocator, ImmutableList.of(rightHashExpression, constant(TRUE, BOOLEAN)), ImmutableList.of());

            VariableReferenceExpression mapAggVariable = variableAllocator.newVariable("expr", mapType);
            PlanNode crossJoinRhs = addAggregation(rightProjectNode, functionAndTypeManager, idAllocator, variableAllocator, "MAP_AGG", mapType, ImmutableList.of(), mapAggVariable, rightProjectNode.getOutputVariables().get(0), rightProjectNode.getOutputVariables().get(1));
            PlanNode crossJoinLhs = addProjections(originalSource, idAllocator, variableAllocator, ImmutableList.of(leftHashExpression), ImmutableList.of());
            ImmutableList.Builder<VariableReferenceExpression> crossJoinOutput = ImmutableList.builder();

            crossJoinOutput.addAll(crossJoinLhs.getOutputVariables());
            crossJoinOutput.addAll(crossJoinRhs.getOutputVariables());

            PlanNode crossJoin = new JoinNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    typeConvert(Join.Type.CROSS),
                    crossJoinLhs,
                    crossJoinRhs,
                    ImmutableList.of(),
                    crossJoinOutput.build(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(REPLICATED),
                    ImmutableMap.of());

            VariableReferenceExpression mapVariable = crossJoinRhs.getOutputVariables().get(0);
            VariableReferenceExpression lookupVariable = crossJoinLhs.getOutputVariables().get(crossJoinLhs.getOutputVariables().size() - 1);
            RowExpression cardinality = call(functionAndTypeManager, "CARDINALITY", BIGINT, mapVariable);
            RowExpression countExpr = constant(count, BIGINT);

            FunctionHandle equalsFunctionHandle = metadata.getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT));
            RowExpression foundAllEntires = call(EQUAL.name(), equalsFunctionHandle, BOOLEAN, cardinality, countExpr);
            RowExpression mapElementAt = call(functionAndTypeManager, "element_at", BOOLEAN, mapVariable, lookupVariable);
            RowExpression check = specialForm(IF, BOOLEAN, foundAllEntires, mapElementAt, constant(TRUE, BOOLEAN));

            FilterNode filterNode = new FilterNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    crossJoin,
                    check);

            Assignments.Builder originalOutputs = Assignments.builder();
            for (VariableReferenceExpression variableReferenceExpression : originalSource.getOutputVariables()) {
                originalOutputs.put(variableReferenceExpression, variableReferenceExpression);
            }

            ProjectNode filteredSource = new ProjectNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    filterNode,
                    originalOutputs.build(),
                    LOCAL);

            return replaceChildren(aggregationNode, ImmutableList.of(filteredSource));
        }
    }
}
