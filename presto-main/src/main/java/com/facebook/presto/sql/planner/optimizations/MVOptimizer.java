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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.MV_OPTIMIZATION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MVOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(MVOptimizer.class);
    private static final HashMap<String, QualifiedObjectName> mvTableNames = new HashMap<>();
    private static final HashMap<String, String> expressionToMVColumnName = new HashMap<>();
    private final Metadata metadata;

    public MVOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        mvTableNames.put("orders", new QualifiedObjectName("hive", "tpch", "rj_mv_orders_derived"));
        mvTableNames.put("rj_mv_base_table_simple", new QualifiedObjectName("hive", "tpch", "rj_mv_mv_table_simple"));
        mvTableNames.put("admetrics_output_nrt", new QualifiedObjectName("prism", "nrt", "rj_mv_admetrics_output_nrt_approx_1"));

        expressionToMVColumnName.put("MULTIPLY(orderkey, custkey)", "_orderkey_mult_custkey_");
        expressionToMVColumnName.put("MULTIPLY(id1, id6)", "_id1_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id2, id6)", "_id2_mult_id6_");
        expressionToMVColumnName.put("MULTIPLY(id3, id6)", "_id3_mult_id6_");
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
        if (!isMVEnabled(session)) {
            return planNode;
        }

        if (!isMVCompatible(planNode)) {
            log.error("The provided plan node is not compatible with supported materialized views.");
            return planNode;
        }

        //TODO build object to get the table name.
        QualifiedObjectName tableQualifiedName = new QualifiedObjectName("hive", "tpch", "rj_mv_base_table_simple");
        QualifiedObjectName mvQualifiedObjectName = mvTableNames.get("rj_mv_base_table_simple");

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableQualifiedName);
        Optional<TableHandle> mvTableHandle = metadata.getTableHandle(session, mvQualifiedObjectName);

        checkState(tableHandle.isPresent());

        //TODO: Use ODS counter.
        if (!mvTableHandle.isPresent()) {
            log.error("MV Table handle is not present. MV Table name: %s", mvQualifiedObjectName);
            return planNode;
        }

        Context mvOptimizerContext = new Context(session, metadata, mvTableHandle.get(), tableHandle.get(), planVariableAllocator);

        return SimplePlanRewriter.rewriteWith(new MVOptimizer.Rewriter(session, metadata, idAllocator), planNode, mvOptimizerContext);
    }

    private boolean isMVCompatible(PlanNode node)
    {
        if (!(node instanceof OutputNode)) { return false; }
        node = ((OutputNode) node).getSource();
        if (!(node instanceof AggregationNode)) { return false; }
        node = ((AggregationNode) node).getSource();
        if (!(node instanceof ProjectNode)) { return false; }
        node = ((ProjectNode) node).getSource();
        if (node instanceof TableScanNode) { return true; }
        if (!(node instanceof FilterNode)) { return false; }
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
        private final List<VariableReferenceExpression> mvCarryVariables;

        private Context(Session session, Metadata metadata, TableHandle mvTableHandle, TableHandle tableHandle, PlanVariableAllocator planVariableAllocator)
        {
            this.mvTableHandle = mvTableHandle;
            this.tableHandle = tableHandle;
            this.tableColumnHandles = metadata.getColumnHandles(session, tableHandle);
            this.mvColumnHandles = metadata.getColumnHandles(session, mvTableHandle);
            this.planVariableAllocator = planVariableAllocator;
            this.mvColumnNameToVariable = new HashMap<>();
            this.mvCarryVariables = new ArrayList<>();
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

        public List<VariableReferenceExpression> getMVCarryVariables()
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
            node.getAssignments().forEach((variable, rowExpression) -> {
                String key = rowExpression.toString();
                if (expressionToMVColumnName.containsKey(key)) {
                    String mvColumnName = expressionToMVColumnName.get(key);
                    if (!mvColumnNameToVariable.containsKey(mvColumnName)) {
                        VariableReferenceExpression newMVColumnVariable = planVariableAllocator.newVariable(mvColumnName, BIGINT);
                        mvColumnNameToVariable.put(mvColumnName, newMVColumnVariable);
                    }
                    assignments.put(variable, mvColumnNameToVariable.get(mvColumnName));
                }
                else {
                    //carry variables.
                }
            });

            ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), node.getSource(), assignments.build(), node.getLocality());
            PlanNode planNode = super.visitProject(projectNode, context);
            return planNode;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            List<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();
            RowExpression predicates = node.getPredicate();
            List<RowExpression> conjuncts = LogicalRowExpressions.extractConjuncts(predicates);
            conjuncts.forEach(predicate -> {
                CallExpression callExpression = (CallExpression) predicate;
                List<RowExpression> arguments = callExpression.getArguments();
                mvCarryVariables.add((VariableReferenceExpression) arguments.get(0));
            });

            return super.visitFilter(node, context);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            TableHandle newMVTableHandle = context.get().getMVTableHandle();
            Map<String, ColumnHandle> mvColumnHandles = context.get().getMVColumnHandles();
            Map<String, ColumnHandle> tableColumnHandles = context.get().getTableColumnHandles();
            List<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> mvAssignment = ImmutableMap.builder();
            List<VariableReferenceExpression> mvOutputVariables = new ArrayList<>();

            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();

            /*
                In the table scan as well, variable expression name can be different than the column handle name.
                From the projection, find out the list of columns need to be replaced.
                From the filter operation, find out list of columns, needs to be propagated.
                Only the columns, which are being replaced needs a new expression variable.
                You must write test to validate each scenario.
             */

            mvCarryVariables.forEach(variable -> {
                //TODO: get the real column name from the variable name.
                ColumnHandle mvColumnHandle = mvColumnHandles.get(variable.getName());
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
