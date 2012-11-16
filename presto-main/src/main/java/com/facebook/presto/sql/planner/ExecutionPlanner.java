package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.LegacyStorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationFunctionStep;
import com.facebook.presto.operator.aggregation.AggregationFunctions;
import com.facebook.presto.operator.aggregation.Input;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;

public class ExecutionPlanner
{
    private final SessionMetadata metadata;
    private final LegacyStorageManager storage;

    public ExecutionPlanner(SessionMetadata metadata, LegacyStorageManager storage)
    {
        this.metadata = metadata;
        this.storage = storage;
    }

    public Operator plan(PlanNode plan)
    {
        if (plan instanceof TableScan) {
            return createTableScan((TableScan) plan);
        }
        else if (plan instanceof ProjectNode) {
            return createProjectNode((ProjectNode) plan);
        }
        else if (plan instanceof FilterNode) {
            return createFilterNode((FilterNode) plan);
        }
        else if (plan instanceof OutputPlan) {
            return createOutputPlan((OutputPlan) plan);
        }
        else if (plan instanceof AggregationNode) {
            return createAggregationNode((AggregationNode) plan);
        }
        else if (plan instanceof LimitNode) {
            return createLimitNode((LimitNode) plan);
        }
        else if (plan instanceof TopNNode) {
            return createTopNNode((TopNNode) plan);
        }

        throw new UnsupportedOperationException("not yet implemented: " + plan.getClass().getName());
    }

    private Operator createOutputPlan(OutputPlan node)
    {
        PlanNode source = Iterables.getOnlyElement(node.getSources());
        Operator sourceOperator = plan(Iterables.getOnlyElement(node.getSources()));

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(source.getOutputs());

        List<Slot> resultSlots = Lists.transform(node.getColumnNames(), forMap(node.getAssignments()));
        if (resultSlots.equals(source.getOutputs())) {
            // no need for a projection -- the output matches the result of the underlying operator
            return sourceOperator;
        }

        List<ProjectionFunction> projections = new ArrayList<>();
        for (Slot slot : resultSlots) {
            ProjectionFunction function = new InterpretedProjectionFunction(slot.getType(), new SlotReference(slot), slotToChannelMappings);
            projections.add(function);
        }

        return new FilterAndProjectOperator(sourceOperator, FilterFunctions.TRUE_FUNCTION, projections);
    }

    private Operator createTopNNode(TopNNode node)
    {
        Preconditions.checkArgument(node.getOrderBy().size() == 1, "Order by multiple fields not yet supported");
        Slot orderBySlot = Iterables.getOnlyElement(node.getOrderBy());

        PlanNode source = Iterables.getOnlyElement(node.getSources());

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(source.getOutputs());

        List<ProjectionFunction> projections = new ArrayList<>();
        for (int i = 0; i < node.getOutputs().size(); i++) {
            Slot slot = node.getOutputs().get(i);
            ProjectionFunction function = ProjectionFunctions.singleColumn(slot.getType().getRawType(), slotToChannelMappings.get(slot), 0);
            projections.add(function);
        }

        Ordering<TupleReadable> ordering = Ordering.from(FieldOrderedTupleComparator.INSTANCE);
        if (node.getOrderings().get(orderBySlot) == SortItem.Ordering.ASCENDING) {
            ordering = ordering.reverse();
        }

        return new TopNOperator(plan(source), (int) node.getCount(), slotToChannelMappings.get(orderBySlot), projections, ordering);
    }

    private Operator createLimitNode(LimitNode node)
    {
        PlanNode source = Iterables.getOnlyElement(node.getSources());
        return new LimitOperator(plan(source), node.getCount());
    }

    private Operator createAggregationNode(AggregationNode node)
    {
        PlanNode source = Iterables.getOnlyElement(node.getSources());
        Operator sourceOperator = plan(source);

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(source.getOutputs());

        List<Provider<AggregationFunctionStep>> aggregationFunctions = new ArrayList<>();
        for (Map.Entry<Slot, FunctionCall> entry : node.getAggregations().entrySet()) {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : entry.getValue().getArguments()) {
                Slot slot = ((SlotReference) argument).getSlot();
                int channel = slotToChannelMappings.get(slot);
                int field = 0; // TODO: support composite channels

                arguments.add(new Input(channel, field));
            }

            Provider<AggregationFunction> boundFunction = node.getFunctionInfos().get(entry.getKey()).bind(arguments);

            Provider<AggregationFunctionStep> aggregation;
            switch (node.getStep()) {
                case PARTIAL:
                    aggregation = AggregationFunctions.partialAggregation(boundFunction);
                    break;
                case FINAL:
                    aggregation = AggregationFunctions.finalAggregation(boundFunction);
                    break;
                case SINGLE:
                    aggregation = AggregationFunctions.singleNodeAggregation(boundFunction);
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getStep());
            }

            aggregationFunctions.add(aggregation);
        }

        List<ProjectionFunction> projections = new ArrayList<>();
        for (int i = 0; i < node.getOutputs().size(); ++i) {
            Slot slot = node.getOutputs().get(i);
            projections.add(ProjectionFunctions.singleColumn(slot.getType().getRawType(), i, 0));
        }

        if (node.getGroupBy().isEmpty()) {
            return new AggregationOperator(sourceOperator, aggregationFunctions, projections);
        }

        Preconditions.checkArgument(node.getGroupBy().size() <= 1, "Only single GROUP BY key supported at this time");
        Slot groupBySlot = Iterables.getOnlyElement(node.getGroupBy());
        return new HashAggregationOperator(sourceOperator, slotToChannelMappings.get(groupBySlot), aggregationFunctions, projections);
    }

    private Operator createFilterNode(FilterNode node)
    {
        PlanNode source = Iterables.getOnlyElement(node.getSources());
        Operator sourceOperator = plan(source);

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(source.getOutputs());

        FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), slotToChannelMappings);

        List<ProjectionFunction> projections = new ArrayList<>();
        for (int i = 0; i < node.getOutputs().size(); i++) {
            Slot slot = node.getOutputs().get(i);
            ProjectionFunction function = ProjectionFunctions.singleColumn(slot.getType().getRawType(), slotToChannelMappings.get(slot), 0);
            projections.add(function);
        }

        return new FilterAndProjectOperator(sourceOperator, filter, projections);
    }

    private Operator createProjectNode(final ProjectNode node)
    {
        PlanNode source = Iterables.getOnlyElement(node.getSources());
        Operator sourceOperator = plan(Iterables.getOnlyElement(node.getSources()));

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(source.getOutputs());

        List<ProjectionFunction> projections = new ArrayList<>();
        for (int i = 0; i < node.getExpressions().size(); i++) {
            Slot slot = node.getOutputs().get(i);
            Expression expression = node.getExpressions().get(i);
            ProjectionFunction function = new InterpretedProjectionFunction(slot.getType(), expression, slotToChannelMappings);
            projections.add(function);
        }

        return new FilterAndProjectOperator(sourceOperator, FilterFunctions.TRUE_FUNCTION, projections);
    }

    private Operator createTableScan(TableScan node)
    {
        TableMetadata tableMetadata = metadata.getTable(QualifiedName.of(node.getCatalogName(), node.getSchemaName(), node.getTableName()));

        Integer[] indicies = new Integer[node.getAttributes().size()];

        Map<Slot, Integer> slotToChannelMappings = mapSlotsToChannels(node.getOutputs());

        for (Map.Entry<String, Slot> entry : node.getAttributes().entrySet()) {
            int channel = slotToChannelMappings.get(entry.getValue());
            indicies[channel] = findIndex(entry.getKey(), tableMetadata);
        }

        // TODO: replace this with DataStreamProvider when plans are operating in terms of handles
        return storage.getOperator(node.getSchemaName(), node.getTableName(), Arrays.asList(indicies));
    }

    public static TupleInfo toTupleInfo(Iterable<Slot> slots)
    {
        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();
        for (Slot slot : slots) {
            types.add(slot.getType().getRawType());
        }
        return new TupleInfo(types.build());
    }

    private Map<Slot, Integer> mapSlotsToChannels(List<Slot> outputs)
    {
        Map<Slot, Integer> slotToChannelMappings = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            slotToChannelMappings.put(outputs.get(i), i);
        }
        return slotToChannelMappings;
    }

    private int findIndex(String columnName, TableMetadata tableMetadata)
    {
        for (int index = 0; index < tableMetadata.getColumns().size(); index++) {
            if (tableMetadata.getColumns().get(index).getName().equals(columnName)) {
                return index;
            }
        }
        throw new IllegalArgumentException("Unknown column name: " + columnName);
    }
}
