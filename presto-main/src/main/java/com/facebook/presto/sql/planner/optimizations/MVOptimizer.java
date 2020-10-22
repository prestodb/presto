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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.util.DateTimeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.facebook.presto.SystemSessionProperties.MV_OPTIMIZATION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.MVOptimizerUtils.getFilterNode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MVOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(MVOptimizer.class);
    private final MVRegistry mvRegistry;
    private final Metadata metadata;
    QualifiedObjectName baseTableObject;

    //TODO: Its not working
    MVOptimizerStats mvOptimizerStats = new MVOptimizerStats();
    private final FunctionAndTypeManager functionAndTypeManager;

    public MVOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
        this.mvRegistry = new MVRegistry();

        //TODO: generate this from the planNode.
        baseTableObject = new QualifiedObjectName("hive", "tpch", "simple_base_table");
        //baseTableObject = new QualifiedObjectName("prism", "nrt", "admetrics_output_nrt");
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

        if (!isPlanCompatible(planNode)) {
            mvOptimizerStats.incrementMVMiss();
            return planNode;
        }

        List<MVInfo> mvInfos = mvRegistry.getRegistry(baseTableObject);
        if (mvInfos == null || mvInfos.isEmpty()) {
            log.info("No MV is registered for the table: " + baseTableObject);
            mvOptimizerStats.incrementMVMiss();
            return planNode;
        }

        MVInfo mvInfo = getBestMVInfo(session, planNode, mvInfos);
        if (mvInfo == null) {
            log.info("No suitable MV found for the table: " + baseTableObject);
            mvOptimizerStats.incrementMVMiss();
            return planNode;
        }

        Context mvOptimizerContext = new Context(session, metadata, planVariableAllocator, mvInfo);

        PartitionRange partitionRange = getPartitionRange(planNode);
        if (partitionRange == null) {
            mvOptimizerStats.incrementMVMiss();
            return planNode;
        }

        List<String> landedPartitionList = isPartitionLanded(mvOptimizerContext, mvInfo, partitionRange);

        //TODO: The assumption is that if the partition is landed then the latest ds is in the MV, other wise
        //The latest_ds-1 is in the MV.

        log.info("Using the MV to optimize the query.");
        mvOptimizerStats.incrementMVHit();

        //TODO fix the filters.
        return getMvOptimizedNode(planNode, session, idAllocator, mvOptimizerContext);
    }

    private PlanNode getMvOptimizedNode(PlanNode planNode, Session session, PlanNodeIdAllocator idAllocator, Context mvOptimizerContext)
    {
        return SimplePlanRewriter.rewriteWith(new MVRewriter(session, metadata, idAllocator), planNode, mvOptimizerContext);
    }

    //TODO: Doesn't look like a best plan
    private static class PartitionRange
    {
        public PartitionRange()
        {
            partitionColumn = "ds";
            partitions = new TreeSet<>(Comparator.comparingInt(DateTimeUtils::parseDate));
        }

        String partitionColumn;
        TreeSet<String> partitions;
    }

    private PartitionRange getPartitionRange(PlanNode planNode)
    {
        PartitionRange partitionRange = new PartitionRange();
        List<RowExpression> partitionPredicates = new ArrayList<>();
        FilterNode filterNode = getFilterNode(planNode);

        List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(filterNode.getPredicate());
        for (RowExpression predicate : predicates) {
            if (predicate instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) predicate;
                List<RowExpression> arguments = callExpression.getArguments();
                for (RowExpression expression : arguments) {
                    if (expression instanceof VariableReferenceExpression) {
                        if (partitionRange.partitionColumn.equals(((VariableReferenceExpression) expression).getName())) {
                            partitionPredicates.add(predicate);
                        }
                    }
                }
            }
        }

        if (partitionPredicates.isEmpty()) {
            log.error("Partition predicates are empty");
            return null;
        }
        RowExpression partitionPredicate = partitionPredicates.get(0);
        if (partitionPredicate instanceof CallExpression) {
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(((CallExpression) partitionPredicate).getFunctionHandle());
            Optional<OperatorType> operatorType = functionMetadata.getOperatorType();
            if (!operatorType.isPresent()) {
                log.error("Operator is not present!");
                return null;
            }

            if (operatorType.get().equals(OperatorType.EQUAL)) {
                List<RowExpression> arguments = ((CallExpression) partitionPredicate).getArguments();
                for (RowExpression argument : arguments) {
                    if (argument instanceof VariableReferenceExpression) {
                        partitionRange.partitionColumn = ((VariableReferenceExpression) argument).getName();
                        continue;
                    }
                    checkArgument(argument instanceof CallExpression);
                    Slice slice = (Slice) ((ConstantExpression) ((CallExpression) argument).getArguments().get(0)).getValue();
                    partitionRange.partitions.add(slice.toStringUtf8());
                }
            }
            else if (operatorType.get().equals(OperatorType.GREATER_THAN)) {
                List<RowExpression> arguments = ((CallExpression) partitionPredicate).getArguments();
                for (RowExpression argument : arguments) {
                    if (argument instanceof VariableReferenceExpression) {
                        continue;
                    }
                    checkArgument(argument instanceof CallExpression);
                    Slice slice = (Slice) ((ConstantExpression) ((CallExpression) argument).getArguments().get(0)).getValue();
                    //TODO: Correct it.
                    partitionRange.min = slice.toStringUtf8();
                }
            }
            else {
                log.error("Only Equal operator is supported!");
                return null;
            }
        }
        else {
            log.error("Partition predicates were not of right type");
            return null;
        }

        return partitionRange;
    }

    private MVInfo getBestMVInfo(Session session, PlanNode planNode, List<MVInfo> mvInfos)
    {
        //TODO: All the expression and filter used in the query must be present in the MV.
        for (MVInfo mvInfo : mvInfos) {
            QualifiedObjectName mvObject = mvInfo.getMvObject();
            Optional<TableHandle> mvTableHandle = metadata.getTableHandle(session, mvObject);
            if (!mvTableHandle.isPresent()) {
                log.error("MV Table handle is not present. MV Table name: %s", mvObject);
                continue;
            }

            //TODO: Optimize it by storing information in the context, plan node does not need to be parsed for every MV.
            Map<String, ColumnHandle> mvColumnHandles = metadata.getColumnHandles(session, mvTableHandle.get());
            if (isProjectionCompatible(planNode, mvInfo.getExpressionToMVColumnName(), mvColumnHandles) && isFilterCompatible(planNode, mvColumnHandles)) {
                return mvInfo;
            }
        }

        return null;
    }

    private boolean isFilterCompatible(PlanNode node, Map<String, ColumnHandle> mvColumnHandles)
    {
        FilterNode filterNode = getFilterNode(node);
        List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(filterNode.getPredicate());
        for (RowExpression predicate : predicates) {
            if (predicate instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) predicate;
                List<RowExpression> arguments = callExpression.getArguments();
                for (RowExpression expression : arguments) {
                    if (expression instanceof VariableReferenceExpression) {
                        if (!mvColumnHandles.containsKey(((VariableReferenceExpression) expression).getName())) {
                            log.error(String.format("Failed to find the expression in the dimension![%s], Skipping the MV optimization", expression.toString()));
                            return false;
                        }
                    }
                }
            }
            else if (predicate instanceof SpecialFormExpression) {
                SpecialFormExpression specialFormExpression = (SpecialFormExpression) predicate;
                List<RowExpression> arguments = specialFormExpression.getArguments();
                for (RowExpression expression : arguments) {
                    if (expression instanceof VariableReferenceExpression) {
                        if (!mvColumnHandles.containsKey(((VariableReferenceExpression) expression).getName())) {
                            log.error(String.format("Failed to find the expression in the dimension![%s], Skipping the MV optimization", expression.toString()));
                            return false;
                        }
                    }
                }
            }
            else {
                log.error("Unsupported predicate expression [{}]", predicate);
                return false;
            }
        }

        return true;
    }

    //TODO complete it.. This it carefully. how do you do ds- start to finish loop
    private List<String> isPartitionLanded(Context context, MVInfo mvInfo, PartitionRange partitionRange)
    {
        //TODO: Is this expansive?
        Optional<List<String>> partitionNames = metadata.getPartitionNames(context.getSession(), mvInfo.getMvObject().getSchemaName(), context.getMVTableHandle());

//        String lookupPartition = String.format("%s=%s", partitionRange.partitionColumn, partitionRange.max);
//        if (partitionNames.isPresent()) {
//            List<String> partitions = partitionNames.get();
//            for (String partition : partitions) {
//                if (partition.contains(lookupPartition)) {
//                    return true;
//                }
//            }
//        }
//
//        return false;
    }

    private boolean isProjectionCompatible(PlanNode planNode, Map<String, String> expressionToMVColumnName, Map<String, ColumnHandle> mvColumnHandles)
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
                    if (!mvColumnHandles.containsKey(expressionToMVColumnName.get(key))) {
                        log.info(String.format("Derived column [%s] is not present in the MV", expressionToMVColumnName.get(key)));
                        return false;
                    }
                }
                else if (rowExpression instanceof VariableReferenceExpression) {
                    if (!mvColumnHandles.containsKey(key)) {
                        log.info(String.format("column [%s] is not present in the MV", key));
                        return false;
                    }
                }
                else {
                    log.info("Unsupported [%s] expression for MV optimization. ", key);
                    return false;
                }
            }
        }
        else {
            return planNode.getSources().size() == 1 && isProjectionCompatible(planNode.getSources().get(0), expressionToMVColumnName, mvColumnHandles);
        }

        return true;
    }

    //TODO: Is there a better way? visitor?
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
        private final Session session;
        private final TableHandle tableHandle;
        private final TableHandle mvTableHandle;
        private final Map<String, ColumnHandle> tableColumnHandles;
        private final Map<String, ColumnHandle> mvColumnHandles;
        private final PlanVariableAllocator planVariableAllocator;
        private final Map<String, VariableReferenceExpression> mvColumnNameToVariable;
        private final Set<VariableReferenceExpression> mvCarryVariables;
        private final MVInfo mvInfo;

        private Context(
                Session session,
                Metadata metadata,
                PlanVariableAllocator planVariableAllocator,
                MVInfo mvInfo)
        {
            this.session = session;
            this.mvInfo = mvInfo;
            this.mvTableHandle = metadata.getTableHandle(session, mvInfo.getMvObject()).orElse(null);
            this.tableHandle = metadata.getTableHandle(session, mvInfo.getBaseObject()).orElse(null);
            this.tableColumnHandles = metadata.getColumnHandles(session, this.tableHandle);
            this.mvColumnHandles = metadata.getColumnHandles(session, mvTableHandle);
            this.planVariableAllocator = planVariableAllocator;
            this.mvColumnNameToVariable = new HashMap<>();
            this.mvCarryVariables = new HashSet<>();
        }

        public Session getSession()
        {
            return session;
        }

        public TableHandle getTableHandle()
        {
            return tableHandle;
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

        public MVInfo getMvInfo()
        {
            return mvInfo;
        }
    }

    private boolean isMVEnabled(Session session)
    {
        return session.getSystemProperty(MV_OPTIMIZATION_ENABLED, Boolean.class);
    }

    private static class MVRewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;

        public MVRewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.variableAllocator = new PlanVariableAllocator();
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            PlanNode mvProjectNode = getMVProjectNode(node, context);
            PlanNode baseProjectNode = getBaseProjectNode(node, context);
            return getUnionNode(mvProjectNode, baseProjectNode);
        }

        @NotNull
        private UnionNode getUnionNode(PlanNode mvOptimizedNode, PlanNode baseNode)
        {
            ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputsMapBuilder = ImmutableMap.builder();
            baseNode.getOutputVariables().forEach(variable -> outputToInputsMapBuilder.put(variable, ImmutableList.of(variable, variable)));
            //TODO: It does not work currently.
            return new UnionNode(idAllocator.getNextId(), ImmutableList.of(mvOptimizedNode, baseNode), baseNode.getOutputVariables(), outputToInputsMapBuilder.build());
        }

        private PlanNode getBaseProjectNode(ProjectNode node, RewriteContext<Context> context)
        {
            return node;
        }

        private PlanNode getMVProjectNode(ProjectNode node, RewriteContext<Context> context)
        {
            Assignments.Builder assignments = Assignments.builder();
            PlanVariableAllocator planVariableAllocator = context.get().getPlanVariableAllocator();
            Map<String, VariableReferenceExpression> mvColumnNameToVariable = context.get().getMVColumnNameToVariable();
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();
            Map<String, String> expressionToMVColumnName = context.get().getMvInfo().getExpressionToMVColumnName();

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
            //TODO: Change the filter condition for the ds column
            Set<VariableReferenceExpression> mvCarryVariables = context.get().getMVCarryVariables();
            List<RowExpression> predicates = LogicalRowExpressions.extractPredicates(node.getPredicate());

            predicates.forEach(predicate -> {
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
