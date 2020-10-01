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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.MV_OPTIMIZATION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MVOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(MVOptimizer.class);
    private static final HashMap<String, MVInfo> mvTableNames = new HashMap<>();
    private static final HashMap<String, String> expressionToMVColumnName = new HashMap<>();
    private final Metadata metadata;
    private final String baseTableName;

    public MVOptimizer(Metadata metadata)
    {
        log.info("Initiating MVOptimizer!");
        this.metadata = requireNonNull(metadata, "metadata is null");

        MVInfo rjTableMVInfo = new MVInfo(new QualifiedObjectName("hive", "tpch", "rj_mv_base_table_simple"),
                new QualifiedObjectName("hive", "tpch", "rj_mv_mv_table_simple"));

        MVInfo simpleTableMVInfo = new MVInfo(new QualifiedObjectName("hive", "tpch", "simple_base_table"),
                new QualifiedObjectName("hive", "tpch", "simple_mv_table"));

        MVInfo adMetricsMVInfo = new MVInfo(new QualifiedObjectName("prism", "nrt", "admetrics_output_nrt"),
                new QualifiedObjectName("prism", "nrt", "rj_mv_admetrics_output_nrt_multi_expr"));

        mvTableNames.put("rj_mv_base_table_simple", rjTableMVInfo);
        expressionToMVColumnName.put("MULTIPLY(id1, id6)", "_id1_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id2, id6)", "_id2_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id3, id6)", "_id3_mult_id6_");

        mvTableNames.put("simple_base_table", simpleTableMVInfo);
        expressionToMVColumnName.put("MULTIPLY(id1, id2)", "_id1_mult_id2_");
        expressionToMVColumnName.put("MULTIPLY(id3, CAST(id2))", "_id3_mult_id2_");

        mvTableNames.put("admetrics_output_nrt", adMetricsMVInfo);

        expressionToMVColumnName.put("MULTIPLY(orderkey, custkey)", "_orderkey_mult_custkey_");
        expressionToMVColumnName.put("MULTIPLY(ads_conversions_down_funnel, weight)", "_ads_conversions_down_funnel_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ads_conversions_in_qrt, weight)", "_ads_conversions_in_qrt_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(ads_xouts, weight)", "_ads_xouts_mult_weight_");
        expressionToMVColumnName.put("MULTIPLY(price_to_value_ratio, CAST(weight))", "_price_to_value_ratio_mult_weight_");

        baseTableName = "simple_base_table";
    }

    class MVInfo
    {
        QualifiedObjectName mvObject;
        QualifiedObjectName baseObject;

        public MVInfo(QualifiedObjectName baseObject, QualifiedObjectName mvObject)
        {
            this.mvObject = mvObject;
            this.baseObject = baseObject;
        }
    }

    @Override
    public PlanNode optimize(
            PlanNode planNode,
            Session session,
            TypeProvider types,
            PlanVariableAllocator planVariableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        log.info("MVOptimizer optimize is called!");
        if (!isMVEnabled(session)) {
            log.info("MVOptimizer is not enabled, returning the rootnode.");
            return planNode;
        }

        if (!isMVCompatible(planNode)) {
            log.error("The provided plan node is not compatible with supported materialized views.");
            return planNode;
        }

        //TODO:
        //Get filter predicate, if ds filter is present, gets its min and max values.
        //Get the latest partition landed on the MV table.
        //Check if the MV has all the partitions landed?
        //Update the table for update MV
        //If yes, use the MV otherwise ignore it.
//        getFilterPredicates(planNode);

        //TODO: Scan last week query and build the dimension and metrics.
        //build the column mapping
        //Run the shadow in the T10 cluster.

        //TODO build object to get the table name.
        MVInfo mvInfo = mvTableNames.get(baseTableName);
        QualifiedObjectName tableQualifiedName = mvInfo.baseObject;
        QualifiedObjectName mvQualifiedObjectName = mvInfo.mvObject;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableQualifiedName);
        Optional<TableHandle> mvTableHandle = metadata.getTableHandle(session, mvQualifiedObjectName);

        log.info("Checking it base table is present.");
        checkState(tableHandle.isPresent(), String.format("Base Table [%s] is not present", tableHandle));

        //TODO: Use ODS counter.
        log.info("Checking it MV table is present.");
        if (!mvTableHandle.isPresent()) {
            log.info("MV table is not present, returning the root node");
            log.error("MV Table handle is not present. MV Table name: %s", mvQualifiedObjectName);
            return planNode;
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, mvTableHandle.get());

        Context mvOptimizerContext = new Context(session, metadata, mvTableHandle.get(), tableHandle.get(), planVariableAllocator);
        log.info("Going to rewrite the plan");
        return SimplePlanRewriter.rewriteWith(new MVOptimizer.Rewriter(session, metadata, idAllocator), planNode, mvOptimizerContext);
    }

    private boolean isMVCompatible(PlanNode node)
    {
        return isPlanCompatible(node) && isProjectionCompatible(node);
    }

    private boolean isProjectionCompatible(PlanNode planNode)
    {
        if (planNode instanceof ProjectNode) {
            ProjectNode node = (ProjectNode) planNode;
            Map<VariableReferenceExpression, RowExpression> nodeAssignments = node.getAssignments().getMap();
            Set<VariableReferenceExpression> variableReferenceExpressions = nodeAssignments.keySet();
            for (VariableReferenceExpression variableReferenceExpression : variableReferenceExpressions) {
                RowExpression rowExpression = nodeAssignments.get(variableReferenceExpression);
                String key = rowExpression.toString();
                if (rowExpression instanceof CallExpression) {
                    if (!expressionToMVColumnName.containsKey(key)) {
                        log.info("Expression [%s] is not registered for MV optimization. ", key);
                        return false;
                    }
                }
                else if (!(rowExpression instanceof VariableReferenceExpression)) {
                    //TODO for variable reference check if the column is present in the MV
                    log.info("Unsupported [%s] expression for MV optimization. ", key);
                    return false;
                }
            }
        }
        else {
            return planNode.getSources().size() == 1 && isProjectionCompatible(planNode.getSources().get(0));
        }

        return true;
    }

    private boolean isPlanCompatible(PlanNode node)
    {
        if (!(node instanceof OutputNode)) {
            return false;
        }
        node = ((OutputNode) node).getSource();
        if (!(node instanceof AggregationNode)) {
            return false;
        }
        node = ((AggregationNode) node).getSource();
        if (!(node instanceof ProjectNode)) {
            return false;
        }
        node = ((ProjectNode) node).getSource();
        if (node instanceof TableScanNode) {
            return true;
        }
        if (!(node instanceof FilterNode)) {
            return false;
        }
        node = ((FilterNode) node).getSource();
        return node instanceof TableScanNode;
    }

    private static class Context
    {
        private final TableHandle tableHandle;
        private final TableHandle mvTableHandle;
        private final Map<String, ColumnHandle> tableColumnHandles;
        private final Map<String, ColumnHandle> mvColumnHandles;
        private final PlanVariableAllocator planVariableAllocator;
        private final Map<String, VariableReferenceExpression> mvColumnNameToVariable;
        private final Set<VariableReferenceExpression> mvCarryVariables;

        private Context(Session session, Metadata metadata, TableHandle mvTableHandle, TableHandle tableHandle, PlanVariableAllocator planVariableAllocator)
        {
            this.mvTableHandle = mvTableHandle;
            this.tableHandle = tableHandle;
            this.tableColumnHandles = metadata.getColumnHandles(session, tableHandle);
            this.mvColumnHandles = metadata.getColumnHandles(session, mvTableHandle);
            this.planVariableAllocator = planVariableAllocator;
            this.mvColumnNameToVariable = new HashMap<>();
            this.mvCarryVariables = new HashSet<>();
        }

        public TableHandle getMVTableHandle()
        {
            return mvTableHandle;
        }

        public PlanVariableAllocator getPlanVariableAllocator()
        {
            return planVariableAllocator;
        }

        public Map<String, ColumnHandle> getTableColumnHandles()
        {
            return tableColumnHandles;
        }

        public Map<String, ColumnHandle> getMVColumnHandles()
        {
            return mvColumnHandles;
        }

        public Map<String, VariableReferenceExpression> getMVColumnNameToVariable()
        {
            return mvColumnNameToVariable;
        }

        public Set<VariableReferenceExpression> getMVCarryVariables()
        {
            return mvCarryVariables;
        }
    }

    private boolean isMVEnabled(Session session)
    {
        return session.getSystemProperty(MV_OPTIMIZATION_ENABLED, Boolean.class);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;
        private final Metadata metadata;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;

        public Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
            this.variableAllocator = new PlanVariableAllocator();
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            Assignments.Builder assignments = Assignments.builder();
            PlanVariableAllocator planVariableAllocator = context.get().getPlanVariableAllocator();
            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();

            try {
                node.getAssignments().forEach((variable, rowExpression) -> {
                    String key = rowExpression.toString();
                    if (rowExpression instanceof CallExpression) {
                        if (expressionToMVColumnName.containsKey(key)) {
                            String mvColumnName = expressionToMVColumnName.get(key);
                            if (!mvColumnNameToVariable.containsKey(mvColumnName)) {
                                VariableReferenceExpression newMVColumnVariable = planVariableAllocator.newVariable(mvColumnName, BIGINT);
                                mvColumnNameToVariable.put(mvColumnName, newMVColumnVariable);
                            }
                            assignments.put(variable, mvColumnNameToVariable.get(mvColumnName));
                        }
                        else {
                            //TODO: move this logic to validation.
                            log.info("Expression [{}] is not registered for MV optimization. ", key);
                            checkState(false);
                        }
                    }
                    else if (rowExpression instanceof VariableReferenceExpression) {
                        mvCarryVariables.add((VariableReferenceExpression) rowExpression);
                        assignments.put(variable, rowExpression);
                    }
                    else {
                        log.info("Unsupported [{}] expression for MV optimization. ", key);
                        checkState(false);
                    }
                });
            }
            catch (IllegalStateException ex) {
                return node;
            }

            ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), node.getSource(), assignments.build(), node.getLocality());
            PlanNode planNode = super.visitProject(projectNode, context);
            return planNode;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();
            RowExpression predicates = node.getPredicate();
            List<RowExpression> conjuncts = LogicalRowExpressions.extractConjuncts(predicates);
            conjuncts.forEach(predicate -> {
                if (predicate instanceof CallExpression) {
                    CallExpression callExpression = (CallExpression) predicate;
                    List<RowExpression> arguments = callExpression.getArguments();
                    arguments.stream().filter(expression -> expression instanceof VariableReferenceExpression).forEach(expression -> mvCarryVariables.add((VariableReferenceExpression) expression));
                }
                else if (predicate instanceof SpecialFormExpression) {
                    SpecialFormExpression specialFormExpression = (SpecialFormExpression) predicate;
                    List<RowExpression> arguments = specialFormExpression.getArguments();
                    arguments.stream().filter(expression -> expression instanceof VariableReferenceExpression).forEach(expression -> mvCarryVariables.add((VariableReferenceExpression) expression));
                }
                else {
                    log.error("Unsupported predicate expression [{}]", predicate);
                }
            });

            return super.visitFilter(node, context);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            TableHandle newMVTableHandle = context.get().getMVTableHandle();
            Map<String, ColumnHandle> mvColumnHandles = context.get().getMVColumnHandles();
            Map<String, ColumnHandle> tableColumnHandles = context.get().getTableColumnHandles();
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> mvAssignment = ImmutableMap.builder();
            List<VariableReferenceExpression> mvOutputVariables = new ArrayList<>();

            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();

            mvCarryVariables.forEach(variable -> {
                String columnName = tableColumnHandles.get(variable.getName()).getName();
                ColumnHandle mvColumnHandle = mvColumnHandles.get(columnName);
                mvAssignment.put(variable, mvColumnHandle);
                mvOutputVariables.add(variable);
            });

            mvColumnNameToVariable.forEach((columnName, variable) -> {
                ColumnHandle mvColumnHandle = mvColumnHandles.get(columnName);
                mvAssignment.put(variable, mvColumnHandle);
                mvOutputVariables.add(variable);
            });

            //TODO: Use real constraints
            return new TableScanNode(
                    node.getId(),
                    newMVTableHandle,
                    mvOutputVariables,
                    mvAssignment.build(),
                    TupleDomain.all(),
                    TupleDomain.all());
        }
    }
}
