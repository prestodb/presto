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
package com.facebook.presto.sql.planner.iterative.properties;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.relational.FunctionResolution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Defines a suite of plan node-specific methods for the computation of logical properties.
 * Supplies a default implementation that produces an empty set of logical properties, and additionally,
 * a suite of plan-node specific overrides of the default implementation. The implementations leverage
 * property propagation builders supplied by LogicalPropertiesImpl. The LogicalPropertiesProvider
 * mechanism enables a plan node to receive its logical property compute capabilities via dependency injection.
 * This is needed because the computation of logical properties requires analysis of plan node's argument expressions,
 * and the code the performs this analysis must be built in presto-main as this is where expression related classes are visible.
 * The property computation implementation is dynamically injected into the presto-spi and presto-main plan node method's responsible
 * for computing logical properties.
 */
public class LogicalPropertiesProviderImpl
        implements LogicalPropertiesProvider
{
    private final FunctionResolution functionResolution;

    public LogicalPropertiesProviderImpl(FunctionResolution functionResolution)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    /**
     * Provides the logical properties for a ValuesNode. Bounds the MaxCard property to the row count.
     *
     * @param valuesNode
     * @return The logical properties for a ValuesNode.
     */
    @Override
    public LogicalProperties getValuesProperties(ValuesNode valuesNode)
    {
        LogicalPropertiesImpl sourceProps = new LogicalPropertiesImpl.NoPropagateBuilder(functionResolution).build();
        LogicalPropertiesImpl.PropagateAndLimitBuilder propagateAndLimitBuilder = new LogicalPropertiesImpl.PropagateAndLimitBuilder(sourceProps, valuesNode.getRows().size(), functionResolution);
        return propagateAndLimitBuilder.build();
    }

    /**
     * Provides the logical properties for a TableScanNode. These properties come from analysis of catalog constraints.
     * The current implementation is just scaffolding that will be removed once the HMS upgrade is completed.
     *
     * @param tableScanNode
     * @return The logical properties for a TableScanNode.
     */
    @Override
    public LogicalProperties getTableScanProperties(TableScanNode tableScanNode)
    {
        // map primary key and unique constraints from column handles to variable reference expressions
        List<Set<VariableReferenceExpression>> keys = new ArrayList<>();
        List<TableConstraint<ColumnHandle>> uniqueConstraints = tableScanNode.getTableConstraints().stream().filter(tableConstraint -> tableConstraint instanceof UniqueConstraint && (tableConstraint.isEnforced() || tableConstraint.isRely())).collect(Collectors.toList());
        if (!uniqueConstraints.isEmpty()) {
            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScanNode.getAssignments();
            Map<ColumnHandle, VariableReferenceExpression> inverseAssignments = assignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            uniqueConstraints.stream().filter(uniqueConstraint -> uniqueConstraint.getColumns().stream().allMatch(col -> inverseAssignments.containsKey(col))).forEach(uniqueConstraint -> keys.add(uniqueConstraint.getColumns().stream().map(col -> inverseAssignments.get(col)).collect(Collectors.toSet())));
        }
        LogicalPropertiesImpl.TableScanBuilder logicalPropsBuilder = new LogicalPropertiesImpl.TableScanBuilder(keys, functionResolution);
        return logicalPropsBuilder.build();
    }

    /**
     * Provides the logical properties for a FilterNode. These properties reflect the effects of applying predicates to the source properties.
     *
     * @param filterNode
     * @return The logical properties for a FilterNode.
     */
    @Override
    public LogicalProperties getFilterProperties(FilterNode filterNode)
    {
        if (!((filterNode.getSource() instanceof GroupReference) && ((GroupReference) filterNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }
        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) filterNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.FilterBuilder logicalPropsBuilder = new LogicalPropertiesImpl.FilterBuilder(sourceProps, filterNode.getPredicate(), functionResolution);
        return logicalPropsBuilder.build();
    }

    /**
     * Provides the logical properties for a ProjectNode. These properties are essentially a projection and reassignment of the
     * variable references in the source properties.
     *
     * @param projectNode
     * @return The logical properties for a ProjectNode.
     */
    @Override
    public LogicalProperties getProjectProperties(ProjectNode projectNode)
    {
        if (!((projectNode.getSource() instanceof GroupReference) && ((GroupReference) projectNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }
        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) projectNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.ProjectBuilder logicalPropsBuilder = new LogicalPropertiesImpl.ProjectBuilder(sourceProps, projectNode.getAssignments(), functionResolution);
        return logicalPropsBuilder.build();
    }

    /**
     * Provides the logical properties for a JoinNode. These properties reflect the effects of combining the properties of the left and right sources.
     *
     * @param node An instance of JoinNode.
     * @return The logical properties for a JoinNode.
     */
    @Override
    public LogicalProperties getJoinProperties(PlanNode node)
    {
        if (!(node instanceof JoinNode)) {
            throw new IllegalArgumentException("Expected PlanNode to be instance of JoinNode");
        }

        JoinNode joinNode = (JoinNode) node;
        if (!((joinNode.getLeft() instanceof GroupReference) && ((GroupReference) joinNode.getLeft()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected left source PlanNode to be a GroupReference with LogicalProperties");
        }

        if (!((joinNode.getRight() instanceof GroupReference) && ((GroupReference) joinNode.getRight()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected right source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl leftProps = (LogicalPropertiesImpl) ((GroupReference) joinNode.getLeft()).getLogicalProperties().get();
        LogicalPropertiesImpl rightProps = (LogicalPropertiesImpl) ((GroupReference) joinNode.getRight()).getLogicalProperties().get();
        LogicalPropertiesImpl.JoinBuilder logicalPropsBuilder = new LogicalPropertiesImpl.JoinBuilder(leftProps, rightProps, joinNode.getCriteria(), joinNode.getType(), joinNode.getFilter(), joinNode.getOutputVariables(), functionResolution);
        return logicalPropsBuilder.build();
    }

    /**
     * Provides the logical properties for a SemiJoinNode. The properties of the non-filtering source are propagated without change.
     *
     * @param node An instance of SemiJoinNode.
     * @return The logical properties for a SemiJoinNode.
     */
    @Override
    public LogicalProperties getSemiJoinProperties(PlanNode node)
    {
        if (!(node instanceof SemiJoinNode)) {
            throw new IllegalArgumentException("Expected PlanNode to be instance of SemiJoinNode");
        }

        SemiJoinNode semiJoinNode = (SemiJoinNode) node;
        if (!((semiJoinNode.getSource() instanceof GroupReference) && ((GroupReference) semiJoinNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected non-filtering source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) semiJoinNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.PropagateBuilder propagateBuilder = new LogicalPropertiesImpl.PropagateBuilder(sourceProps, functionResolution);
        return propagateBuilder.build();
    }

    /**
     * Provides the logical properties for an AggregationNode. If a distinct or grouping operation is being performed then the logical properties
     * reflect the addition of a unique key to the source properties.
     *
     * @param aggregationNode
     * @return The logical properties for an AggregationNode.
     */
    @Override
    public LogicalProperties getAggregationProperties(AggregationNode aggregationNode)
    {
        if (!((aggregationNode.getSource() instanceof GroupReference) && ((GroupReference) aggregationNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }

        if (aggregationNode.getGroupingKeys().isEmpty() && aggregationNode.getAggregations().isEmpty()) {
            throw new IllegalStateException("Aggregation node with no grouping columns and no aggregation functions");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) aggregationNode.getSource()).getLogicalProperties().get();
        if (!aggregationNode.getAggregations().isEmpty() && aggregationNode.getGroupingKeys().isEmpty()) {
            //aggregation with no grouping variables, single row output
            LogicalPropertiesImpl.PropagateAndLimitBuilder propagateBuilder = new LogicalPropertiesImpl.PropagateAndLimitBuilder(sourceProps, Long.valueOf(1), functionResolution);
            return propagateBuilder.build();
        }
        else {
            LogicalPropertiesImpl.AggregationBuilder aggregationBuilder = new LogicalPropertiesImpl.AggregationBuilder(sourceProps,
                    aggregationNode.getGroupingKeys().stream().collect(Collectors.toSet()),
                    aggregationNode.getOutputVariables(),
                    functionResolution);
            return aggregationBuilder.build();
        }
    }

    /**
     * Provides the logical properties for a AssignUniqueId. Adds the id variable as a key.
     *
     * @param node
     * @return The logical properties for a AssignUniqueId.
     */
    @Override
    public LogicalProperties getAssignUniqueIdProperties(PlanNode node)
    {
        if (!(node instanceof AssignUniqueId)) {
            throw new IllegalArgumentException("Expected PlanNode to be instance of AssignUniqueId");
        }

        AssignUniqueId assignUniqueIdNode = (AssignUniqueId) node;
        if (!((assignUniqueIdNode.getSource() instanceof GroupReference) && ((GroupReference) assignUniqueIdNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }

        if (assignUniqueIdNode.getIdVariable() == null) {
            throw new IllegalStateException("AssignUniqueId should have an id variable");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) assignUniqueIdNode.getSource()).getLogicalProperties().get();
        Set<VariableReferenceExpression> key = new HashSet<>();
        key.add(assignUniqueIdNode.getIdVariable());
        LogicalPropertiesImpl.AggregationBuilder aggregationBuilder = new LogicalPropertiesImpl.AggregationBuilder(sourceProps, key, assignUniqueIdNode.getOutputVariables(), functionResolution);
        return aggregationBuilder.build();
    }

    /**
     * Provides the logical properties for a DistinctLimitNode. The resulting properties reflect the application of both a distinct and limit N to the
     * source properties. The distinct aspect adds a unique key to the source properties and the limit bounds the result to N.
     *
     * @param distinctLimitNode
     * @return The logical properties for an DistinctLimitNode.
     */
    @Override
    public LogicalProperties getDistinctLimitProperties(DistinctLimitNode distinctLimitNode)
    {
        if (!((distinctLimitNode.getSource() instanceof GroupReference) && ((GroupReference) distinctLimitNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) distinctLimitNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.DistinctLimitBuilder aggregationBuilder = new LogicalPropertiesImpl.DistinctLimitBuilder(sourceProps,
                distinctLimitNode.getDistinctVariables().stream().collect(Collectors.toSet()),
                distinctLimitNode.getLimit(),
                distinctLimitNode.getOutputVariables(),
                functionResolution);
        return aggregationBuilder.build();
    }

    /**
     * Provides the logical properties for a LimitNode. The properties reflect the application of a limit N to the source properties.
     *
     * @param limitNode
     * @return The logical properties for a LimitNode.
     */
    @Override
    public LogicalProperties getLimitProperties(LimitNode limitNode)
    {
        if (!((limitNode.getSource() instanceof GroupReference) && ((GroupReference) limitNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) limitNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.PropagateAndLimitBuilder propagateBuilder = new LogicalPropertiesImpl.PropagateAndLimitBuilder(sourceProps, limitNode.getCount(), functionResolution);
        return propagateBuilder.build();
    }

    /**
     * Provides the logical properties for a LimitNode. The properties reflect the application of a limit N to the source properties.
     *
     * @param topNNode
     * @return The logical properties for a LimitNode.
     */
    @Override
    public LogicalProperties getTopNProperties(TopNNode topNNode)
    {
        if (!((topNNode.getSource() instanceof GroupReference) && ((GroupReference) topNNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected left source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) topNNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.PropagateAndLimitBuilder propagateBuilder = new LogicalPropertiesImpl.PropagateAndLimitBuilder(sourceProps, topNNode.getCount(), functionResolution);
        return propagateBuilder.build();
    }

    /**
     * Provides the logical properties for a SortNode. The properties of the source are propagated without change.
     *
     * @param node An instance of SortNode.
     * @return The logical properties for a SortNode.
     */
    @Override
    public LogicalProperties getSortProperties(PlanNode node)
    {
        if (!(node instanceof SortNode)) {
            throw new IllegalArgumentException("Expected PlanNode to be instance of SortNode");
        }

        SortNode sortNode = (SortNode) node;
        if (!((sortNode.getSource() instanceof GroupReference) && ((GroupReference) sortNode.getSource()).getLogicalProperties().isPresent())) {
            throw new IllegalStateException("Expected source PlanNode to be a GroupReference with LogicalProperties");
        }

        LogicalPropertiesImpl sourceProps = (LogicalPropertiesImpl) ((GroupReference) sortNode.getSource()).getLogicalProperties().get();
        LogicalPropertiesImpl.PropagateBuilder propagateBuilder = new LogicalPropertiesImpl.PropagateBuilder(sourceProps, functionResolution);
        return propagateBuilder.build();
    }

    /**
     * Provides the default logical properties for a generic PlanNode which is essentially an empty set of properties.
     *
     * @return The default set of logical properties for a generic PlanNode.
     */
    @Override
    public LogicalProperties getDefaultProperties()
    {
        LogicalPropertiesImpl.NoPropagateBuilder logicalPropsBuilder = new LogicalPropertiesImpl.NoPropagateBuilder(functionResolution);
        return logicalPropsBuilder.build();
    }
}
