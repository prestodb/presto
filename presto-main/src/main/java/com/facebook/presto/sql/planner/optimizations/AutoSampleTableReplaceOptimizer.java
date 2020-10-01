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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableSample;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.ApproxResultsOption;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class AutoSampleTableReplaceOptimizer
        implements PlanOptimizer
{
    protected final Metadata metadata;

    public AutoSampleTableReplaceOptimizer(Metadata metadata)
    {
        this(metadata, false);
    }

    public AutoSampleTableReplaceOptimizer(Metadata metadata, boolean failOpen)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    public Optional<TableSample> getSampleTable(Session session, TableHandle tableHandle)
    {
        // Check if the current table has any samples and use them if available
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle).getMetadata();

        List<TableSample> sampledTables = tableMetadata.getSampledTables();
        if (sampledTables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(sampledTables.get(0));
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        ApproxResultsOption approxResultsOption = SystemSessionProperties.getApproxResultsOption(session);
        if (!approxResultsOption.name().contains("SAMPLING")) {
            return plan;
        }
        SampledTableReplacer replacer = new SampledTableReplacer(metadata, variableAllocator, idAllocator, session, types);
        SampledTableReplacer.SampleContext ctx = replacer.getContext();
        PlanNode root = SimplePlanRewriter.rewriteWith(replacer, plan, ctx);
        if (ctx.getSamplingFailed()) {
            if (approxResultsOption.name().contains("FAIL_CLOSE")) {
                throw new PrestoException(StandardErrorCode.SAMPLING_UNSUPPORTED_CASE, ctx.getFailureMessage());
            }
            return plan;
        }
        return root;
    }

    private class SampledTableReplacer
            extends SimplePlanRewriter<SampledTableReplacer.SampleContext>
    {
        private final Logger log = Logger.get(SampledTableReplacer.class);
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;
        private final PlanVariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final SampleContext context = new SampleContext();

        private SampledTableReplacer(
                Metadata metadata,
                PlanVariableAllocator variableAllocator,
                PlanNodeIdAllocator idAllocator,
                Session session,
                TypeProvider types)
        {
            this.metadata = metadata;
            this.session = session;
            this.variableAllocator = variableAllocator;
            this.idAllocator = idAllocator;
            this.types = types;
        }

        private SampleContext getContext()
        {
            return context;
        }

        private PlanNode scanTableVariables(TableScanNode node, RewriteContext<SampleContext> context)
        {
            TableHandle tableHandle = node.getTable();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle).getMetadata();

            String samplingColumn = "";
            boolean isSampleTable = false;
            if (context.get().getTableSample().isPresent()) {
                isSampleTable = context.get().getTableSample().get().getTableName().equals(tableMetadata.getTable());
                if (context.get().getTableSample().get().getSamplingColumnName().isPresent()) {
                    samplingColumn = context.get().getTableSample().get().getSamplingColumnName().get();
                }
            }
            for (VariableReferenceExpression vre : node.getAssignments().keySet()) {
                ColumnMetadata cm = metadata.getColumnMetadata(session, tableHandle, node.getAssignments().get(vre));
                context.get().getMap().put(
                        vre.getName(),
                        new SampleInfo(
                                isSampleTable && cm.getName().equals(samplingColumn),
                                isSampleTable));
            }
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<SampleContext> context)
        {
            if (!session.getCatalog().isPresent()) {
                return node;
            }

            if (!(node.getCurrentConstraint().equals(TupleDomain.all()) && node.getEnforcedConstraint().equals(TupleDomain.all()))) {
                // Not handling it here. Hopefully this optimizer will run before any other optimizer that adds these
                // constraints, so that they (and any table layouts) will get added after this transformation.
                context.get().setFailed("CurrentConstraint or EnforcedConstraint set on TableScanNode");
                return node;
            }

            // If a sampled table is already being used, then just can scan the variables
            if (context.get().getTableSample().isPresent()) {
                return scanTableVariables(node, context);
            }

            // Check if the current table has any samples and use them if available
            Optional<TableSample> sampleTable = getSampleTable(session, node.getTable());
            if (!sampleTable.isPresent()) {
                return scanTableVariables(node, context);
            }
            QualifiedObjectName name = new QualifiedObjectName(
                    session.getCatalog().get(),
                    sampleTable.get().getTableName().getSchemaName(),
                    sampleTable.get().getTableName().getTableName());
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
            if (!tableHandle.isPresent()) {
                log.warn("Could not create table handle for " + sampleTable);
                return scanTableVariables(node, context);
            }

            Map<String, ColumnHandle> sampleColumnHandles = metadata.getColumnHandles(session, tableHandle.get());
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
            for (VariableReferenceExpression vre : node.getAssignments().keySet()) {
                ColumnMetadata cm = metadata.getColumnMetadata(session, node.getTable(), node.getAssignments().get(vre));
                if (!sampleColumnHandles.containsKey(cm.getName())) {
                    log.warn("Sample Table " + sampleTable.get().getTableName() + " does not contain column " + cm.getName());
                    return node;
                }
                columns.put(vre, sampleColumnHandles.get(cm.getName()));
            }

            context.get().setTableSample(sampleTable.get());
            context.get().setShouldScale();

            TableScanNode newNode = new TableScanNode(
                    idAllocator.getNextId(),
                    tableHandle.get(),
                    node.getOutputVariables(),
                    columns.build(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    true);
            return scanTableVariables(newNode, context);
        }

        private ImmutableSet<String> getVariableNames(RowExpression expression)
        {
            ImmutableSet.Builder<String> variablesBuilder = ImmutableSet.builder();
            if (expression instanceof VariableReferenceExpression) {
                variablesBuilder.add(((VariableReferenceExpression) expression).getName());
            }
            else if (isExpression(expression)) {
                ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<ImmutableSet.Builder<String>>() {
                    @Override
                    public Expression rewriteSymbolReference(
                            SymbolReference node,
                            ImmutableSet.Builder<String> context,
                            ExpressionTreeRewriter<ImmutableSet.Builder<String>> treeRewriter)
                    {
                        context.add(node.getName());
                        return node;
                    }}, castToExpression(expression), variablesBuilder);
            }

            ImmutableSet<String> vars = variablesBuilder.build();
            return vars;
        }

        private Optional<SampleInfo> getSampleInfoFromBaseVars(List<RowExpression> exprs, SampleContext context)
        {
            ImmutableSet.Builder<String> baseVarsBuilder = ImmutableSet.builder();
            exprs.forEach(e -> baseVarsBuilder.addAll(getVariableNames(e)));
            ImmutableSet<String> eligibleVars = baseVarsBuilder
                    .build()
                    .stream()
                    .filter(x -> context.getMap().containsKey(x))
                    .collect(toImmutableSet());
            if (eligibleVars.isEmpty()) {
                return Optional.empty();
            }
            boolean isScaleEligible = eligibleVars.stream().anyMatch(s -> context.getMap().get(s).isScaleEligible());
            boolean isSamplingColumn = eligibleVars.stream().anyMatch(s -> context.getMap().get(s).isSamplingColumn());
            return Optional.of(new SampleInfo(isSamplingColumn, isScaleEligible));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<SampleContext> context)
        {
            log.debug("entering project node, id " + node.getId() + ". assignments " + node.getAssignments().getMap() + ". outputs " + node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), context.get());

            if (context.get().getSamplingFailed()) {
                return node;
            }

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                if (context.get().getMap().containsKey(entry.getKey().getName())) {
                    continue;
                }
                Optional<SampleInfo> sio = getSampleInfoFromBaseVars(Collections.singletonList(entry.getValue()), context.get());
                if (sio.isPresent()) {
                    context.get().getMap().put(entry.getKey().getName(), sio.get());
                    log.debug("project node " + node.getId() + ". Adding variable " + entry.getKey() + " to map.");
                }
            }
            log.debug("leaving project node " + node.getId());
            return new ProjectNode(node.getId(), source, node.getAssignments());
        }

        private boolean shouldBeScaled(CallExpression expression)
        {
            String call = expression.getDisplayName();
            return call.equals("sum") || call.equals("count") || call.equals("count_if") || call.equals("approx_distinct");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<SampleContext> context)
        {
            log.debug("Aggregation node, " + node.getId() + ". aggregations " + node.getAggregations() + ". outputs " + node.getOutputVariables());
            PlanNode source = context.rewrite(node.getSource(), context.get());

            if (context.get().getSamplingFailed()) {
                return node;
            }

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> newVariables = ImmutableMap.builder();
            Assignments.Builder assignmentsBuilder = Assignments.builder();

            final Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = node.getAggregations();
            for (VariableReferenceExpression variable : aggregations.keySet()) {
                AggregationNode.Aggregation aggregation = aggregations.get(variable);
                Optional<SampleInfo> si;
                if (aggregation.getArguments().isEmpty() && node.getGroupingSets().getGroupingKeys().isEmpty()) {
                    si = Optional.of(new SampleInfo(false, context.get().getShouldScale()));
                }
                else {
                    si = getSampleInfoFromBaseVars(
                            !aggregation.getArguments().isEmpty() ?
                                    aggregation.getArguments() :
                                    new ArrayList<>(node.getGroupingSets().getGroupingKeys()),
                            context.get());
                }
                if (!si.isPresent()) {
                    continue;
                }

                if (context.get().getTableSample().isPresent() && si.get().isScaleEligible() && shouldBeScaled(aggregation.getCall())) {
                    String scaleFactorDouble = Double.toString(100.0 / context.get().getTableSample().get().getSamplingPercentage().get());
                    String scaleFactorLong = Long.toString(Math.round(100.0 / context.get().getTableSample().get().getSamplingPercentage().get()));
                    VariableReferenceExpression newVar = variableAllocator.newVariable("sample_" + variable.getName(), variable.getType());
                    Expression expr = new ArithmeticBinaryExpression(
                            ArithmeticBinaryExpression.Operator.MULTIPLY, new SymbolReference(newVar.getName()),
                            variable.getType() instanceof BigintType
                                    ? new Cast(new LongLiteral(scaleFactorLong), BigintType.BIGINT.toString())
                                    : new Cast(new DecimalLiteral(scaleFactorDouble), DOUBLE.toString()));
                    assignmentsBuilder.put(
                            variable,
                            OriginalExpressionUtils.castToRowExpression(expr));
                    newVariables.put(variable, newVar);
                }
                else {
                    context.get().getMap().put(variable.getName(), si.get());
                }
            }

            ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> newVars = newVariables.build();

            // If any of the variables being aggregated are eligible for scaling, then
            // add a project node on top of the aggregation node to do the scaleup
            if (!newVars.isEmpty()) {
                SymbolMapper mapper = new SymbolMapper(newVars);
                AggregationNode agn = mapper.map(node, source);

                // Also, Pass the aggregation outputs that are not being aggregated
                agn.getOutputVariables()
                        .stream()
                        .filter(x -> !newVars.containsValue(x))
                        .forEach(x -> assignmentsBuilder.put(x, OriginalExpressionUtils.castToRowExpression(new SymbolReference(x.getName()))));

                ProjectNode pjn = new ProjectNode(idAllocator.getNextId(), agn, assignmentsBuilder.build());
                return pjn;
            }

            return new AggregationNode(
                    node.getId(),
                    source,
                    node.getAggregations(),
                    node.getGroupingSets(),
                    node.getPreGroupedVariables(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<SampleContext> context)
        {
            PlanNode source = context.rewrite(node.getSource(), context.get());
            if (context.get().getSamplingFailed()) {
                return node;
            }

            ImmutableSet<String> predicateVars = getVariableNames(node.getPredicate());

            for (String predVar : predicateVars) {
                SampleInfo si = context.get().getMap().get(predVar);
                if (si != null && si.isSamplingColumn()) {
                    context.get().setFailed("Filtering " + predVar + " based on sampling column");
                    return node;
                }
            }

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<SampleContext> context)
        {
            checkState(!context.get().getTableSample().isPresent(), "TableSample cannot be set on entry to apply node");
            SampleContext inputContext = new SampleContext();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            if (inputContext.getSamplingFailed()) {
                context.get().setFailed(inputContext.getFailureMessage());
                return node;
            }

            SampleContext subQueryContext = new SampleContext();
            PlanNode subQuery = context.rewrite(node.getSubquery(), subQueryContext);

            if (subQueryContext.getSamplingFailed()) {
                context.get().setFailed(subQueryContext.getFailureMessage());
                return node;
            }

            if (!inputContext.getTableSample().isPresent() && !subQueryContext.getTableSample().isPresent()) {
                context.get().getMap().putAll(inputContext.getMap());
                return new ApplyNode(
                        node.getId(),
                        input,
                        subQuery,
                        node.getSubqueryAssignments(),
                        node.getCorrelation(),
                        node.getOriginSubqueryError());
            }

            if (inputContext.getTableSample().isPresent() && subQueryContext.getTableSample().isPresent()) {
                context.get().setFailed("Both input and subQuery of apply are being sampled");
                return node;
            }

            // If subquery was sampled, then the main query vars become eligible for scaling
            if (subQueryContext.getTableSample().isPresent()) {
                context.get().setTableSample(subQueryContext.getTableSample().get());
                if (subQueryContext.getShouldScale()) {
                    context.get().setShouldScale();
                    inputContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                }
                context.get().getMap().putAll(inputContext.getMap());
            }

            if (inputContext.getTableSample().isPresent()) {
                context.get().setTableSample(inputContext.getTableSample().get());
                if (inputContext.getShouldScale()) {
                    context.get().setShouldScale();
                }
                context.get().getMap().putAll(inputContext.getMap());
            }

            return new ApplyNode(
                    node.getId(),
                    input,
                    subQuery,
                    node.getSubqueryAssignments(),
                    node.getCorrelation(),
                    node.getOriginSubqueryError());
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<SampleContext> context)
        {
            checkState(!context.get().getTableSample().isPresent(), "TableSample cannot be set on entry to lateral node");
            SampleContext inputContext = new SampleContext();
            PlanNode input = context.rewrite(node.getInput(), inputContext);

            if (inputContext.getSamplingFailed()) {
                context.get().setFailed(inputContext.getFailureMessage());
                return node;
            }

            SampleContext subQueryContext = new SampleContext();
            PlanNode subQuery = context.rewrite(node.getSubquery(), subQueryContext);

            if (subQueryContext.getSamplingFailed()) {
                context.get().setFailed(subQueryContext.getFailureMessage());
                return node;
            }

            if (!inputContext.getTableSample().isPresent() && !subQueryContext.getTableSample().isPresent()) {
                context.get().getMap().putAll(inputContext.getMap());
                return new LateralJoinNode(
                        node.getId(),
                        input,
                        subQuery,
                        node.getCorrelation(),
                        node.getType(),
                        node.getOriginSubqueryError());
            }

            if (inputContext.getTableSample().isPresent() && subQueryContext.getTableSample().isPresent()) {
                context.get().setFailed("Both input and subQuery of apply are being sampled");
                return node;
            }

            // If subquery was sampled, then the main query vars become eligible for scaling
            if (subQueryContext.getTableSample().isPresent()) {
                context.get().setTableSample(subQueryContext.getTableSample().get());
                if (subQueryContext.getShouldScale()) {
                    context.get().setShouldScale();
                    inputContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                }
                context.get().getMap().putAll(inputContext.getMap());
            }

            if (inputContext.getTableSample().isPresent()) {
                context.get().setTableSample(inputContext.getTableSample().get());
                if (inputContext.getShouldScale()) {
                    context.get().setShouldScale();
                }
                context.get().getMap().putAll(inputContext.getMap());
            }

            return new LateralJoinNode(
                    node.getId(),
                    input,
                    subQuery,
                    node.getCorrelation(),
                    node.getType(),
                    node.getOriginSubqueryError());
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<SampleContext> context)
        {
            context.get().setFailed("Intersect Node not supported yet");
            return node;
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<SampleContext> context)
        {
            context.get().setFailed("Except Node not supported yet");
            return node;
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<SampleContext> context)
        {
            checkState(!context.get().getTableSample().isPresent(), "TableSample cannot be set on entry to union node");

            ImmutableList.Builder<SampleContext> ctxBuilder = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                SampleContext ctx = new SampleContext();
                PlanNode sourceNode = context.rewrite(source, ctx);

                if (ctx.getSamplingFailed()) {
                    context.get().setFailed(ctx.getFailureMessage());
                    return node;
                }

                ctxBuilder.add(ctx);
                rewrittenSources.add(sourceNode);
            }

            // if there has been no sampling, then just return the original node
            ImmutableList<SampleContext> contexts = ctxBuilder.build();
            ImmutableList<PlanNode> sources = rewrittenSources.build();

            Optional<SampleContext> ts = contexts.stream().filter(s -> s.getTableSample().isPresent()).findAny();

            // if there has been no sampling
            if (!ts.isPresent()) {
                return new UnionNode(
                        node.getId(),
                        sources,
                        node.getOutputVariables(),
                        node.getVariableMapping());
            }

            // check is there was sampling from multiple different tables
            Optional<SampleContext> anyOther = contexts
                    .stream()
                    .filter(s -> s.getTableSample().isPresent() && !s.getTableSample().get().equals(ts.get().getTableSample().get()))
                    .findAny();
            if (anyOther.isPresent()) {
                context.get().setFailed("Union between multiple different sample tables "
                        + ts.get().getTableSample().get().getTableName() + ", "
                        + anyOther.get().getTableSample().get().getTableName());
                return node;
            }

            boolean shouldScale = contexts.stream().anyMatch(s -> s.getShouldScale());
            if (!contexts.stream().allMatch(s -> s.getShouldScale() == shouldScale)) {
                context.get().setFailed("Union between sampled and non-sampled set");
                return node;
            }

            contexts.stream().forEach(s -> context.get().getMap().putAll(s.getMap()));
            context.get().setTableSample(ts.get().getTableSample().get());
            if (shouldScale) {
                context.get().setShouldScale();
            }

            return new UnionNode(
                    node.getId(),
                    sources,
                    node.getOutputVariables(),
                    node.getVariableMapping());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<SampleContext> context)
        {
            checkState(!context.get().getTableSample().isPresent(), "TableSample cannot be set on entry to join node");
            SampleContext leftContext = new SampleContext();
            PlanNode left = context.rewrite(node.getLeft(), leftContext);

            if (leftContext.getSamplingFailed()) {
                context.get().setFailed(leftContext.getFailureMessage());
                return node;
            }

            SampleContext rightContext = new SampleContext();
            PlanNode right = context.rewrite(node.getRight(), rightContext);

            if (rightContext.getSamplingFailed()) {
                context.get().setFailed(rightContext.getFailureMessage());
                return node;
            }

            if (!leftContext.getTableSample().isPresent() && !rightContext.getTableSample().isPresent()) {
                context.get().getMap().putAll(leftContext.getMap());
                context.get().getMap().putAll(rightContext.getMap());
                return new JoinNode(
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable(),
                        node.getDistributionType(),
                        node.getDynamicFilters());
            }

            if (leftContext.getTableSample().isPresent() && rightContext.getTableSample().isPresent()) {
                context.get().setFailed("Both left and right side of join are being sampled");
                return node;
            }

            // At this point, the following are the only possibilities
            // 1. Only one side can have getTableSample() set
            // 2. That side may or may not have setShouldScale set

            switch (node.getType()) {
                case INNER:
                    if (leftContext.getTableSample().isPresent()) {
                        context.get().setTableSample(leftContext.getTableSample().get());
                        if (leftContext.getShouldScale()) {
                            rightContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                            context.get().setShouldScale();
                        }
                    }
                    else {
                        context.get().setTableSample(rightContext.getTableSample().get());
                        if (rightContext.getShouldScale()) {
                            leftContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                            context.get().setShouldScale();
                        }
                    }
                    break;
                case LEFT:
                    // If sampled is being left joined with non-sampled, then the non-sampled should scale as well post join
                    if (leftContext.getTableSample().isPresent()) {
                        context.get().setTableSample(leftContext.getTableSample().get());
                        if (leftContext.getShouldScale()) {
                            rightContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                            context.get().setShouldScale();
                        }
                    }
                    else {
                        context.get().setTableSample(rightContext.getTableSample().get());
                    }
                    break;
                case RIGHT:
                    // mirror of the left join case
                    if (rightContext.getTableSample().isPresent()) {
                        context.get().setTableSample(rightContext.getTableSample().get());
                        if (rightContext.getShouldScale()) {
                            context.get().setShouldScale();
                            leftContext.getMap().entrySet().forEach(e -> e.getValue().setScaleEligible(true));
                        }
                    }
                    else {
                        context.get().setTableSample(leftContext.getTableSample().get());
                    }
                    break;
                case FULL:
                    if (leftContext.getTableSample().isPresent()) {
                        context.get().setTableSample(leftContext.getTableSample().get());
                    }
                    else {
                        context.get().setTableSample(rightContext.getTableSample().get());
                    }
                    break;
            }
            context.get().getMap().putAll(leftContext.getMap());
            context.get().getMap().putAll(rightContext.getMap());

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        public class SampleContext
        {
            private final Map<String, SampleInfo> sampleVariables = new HashMap<>();
            private Optional<TableSample> tableSample = Optional.empty();
            private boolean shouldScale;
            private boolean samplingFailed;
            private String failureMessage;

            public void setShouldScale()
            {
                checkState(tableSample.isPresent(), "TableSample should be present if should scale");
                shouldScale = true;
            }

            public boolean getShouldScale()
            {
                return shouldScale;
            }

            public Optional<TableSample> getTableSample()
            {
                return this.tableSample;
            }

            public void setTableSample(TableSample ts)
            {
                checkState(!tableSample.isPresent(), "tableSample should be set only once");
                tableSample = Optional.of(ts);
            }

            public Map<String, SampleInfo> getMap()
            {
                return this.sampleVariables;
            }

            public boolean getSamplingFailed()
            {
                return this.samplingFailed;
            }

            public void setFailed(String failureMessage)
            {
                this.samplingFailed = true;
                this.failureMessage = failureMessage;
            }

            public String getFailureMessage()
            {
                return this.failureMessage;
            }
        }

        private class SampleInfo
        {
            private final boolean isSamplingColumn;
            private boolean isScaleEligible;

            public SampleInfo(boolean isSamplingColumn, boolean isScaleEligible)
            {
                this.isSamplingColumn = isSamplingColumn;
                this.isScaleEligible = isScaleEligible;
            }

            public boolean isSamplingColumn()
            {
                return isSamplingColumn;
            }

            public boolean isScaleEligible()
            {
                return isScaleEligible;
            }

            public void setScaleEligible(boolean isScaleEligible)
            {
                this.isScaleEligible = isScaleEligible;
            }
        }
    }
}
