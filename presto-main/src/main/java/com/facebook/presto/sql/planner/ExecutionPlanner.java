package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.StorageManager;
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
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.DoubleAverageAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.LongAverageAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Functions.forMap;
import static java.lang.String.format;

public class ExecutionPlanner
{
    private final SessionMetadata metadata;
    private final StorageManager storage;

    public ExecutionPlanner(SessionMetadata metadata, StorageManager storage)
    {
        this.metadata = metadata;
        this.storage = storage;
    }

    public Operator plan(PlanNode plan)
    {
        if (plan instanceof AlignNode) {
            return createAlignmentNode((AlignNode) plan);
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

        List<Provider<AggregationFunction>> aggregationFunctions = new ArrayList<>();
        for (Map.Entry<Slot, FunctionCall> entry : node.getAggregations().entrySet()) {
            aggregationFunctions.add(getProvider(node.getFunctionInfos().get(entry.getKey()), entry.getValue(), slotToChannelMappings));
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

    private Provider<AggregationFunction> getProvider(FunctionInfo info, FunctionCall call, Map<Slot, Integer> slotToChannelMappings)
    {
        if (info.getName().equals(QualifiedName.of("count"))) {
            return CountAggregation.PROVIDER;
        }

        if (info.getName().equals(QualifiedName.of("sum"))) {
            Slot input = ((SlotReference) call.getArguments().get(0)).getSlot();
            if (info.getArgumentTypes().get(0) == TupleInfo.Type.FIXED_INT_64) {
                return LongSumAggregation.provider(slotToChannelMappings.get(input), 0);
            }
            else if (info.getArgumentTypes().get(0) == TupleInfo.Type.DOUBLE) {
                return DoubleSumAggregation.provider(slotToChannelMappings.get(input), 0);
            }
        }
        else if (info.getName().equals(QualifiedName.of("avg"))) {
            Slot input = ((SlotReference) call.getArguments().get(0)).getSlot();
            if (info.getArgumentTypes().get(0) == TupleInfo.Type.FIXED_INT_64) {
                return LongAverageAggregation.provider(slotToChannelMappings.get(input), 0);
            }
            else if (info.getArgumentTypes().get(0) == TupleInfo.Type.DOUBLE) {
                return DoubleAverageAggregation.provider(slotToChannelMappings.get(input), 0);
            }
        }

        throw new UnsupportedOperationException(format("not yet implemented: %s(%s)", info.getName(), Joiner.on(", ").join(info.getArgumentTypes())));
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

    private Operator createAlignmentNode(AlignNode node)
    {
        BlockIterable[] blocks = new BlockIterable[node.getSources().size()];

        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);

            ColumnScan scan = (ColumnScan) source;

            TableMetadata tableMetadata = metadata.getTable(QualifiedName.of(scan.getCatalogName(), scan.getSchemaName(), scan.getTableName()));
            int index = 0;
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                if (columnMetadata.getName().equals(scan.getAttributeName())) {
                    break;
                }
                ++index;
            }

            blocks[i] = storage.getBlocks(scan.getSchemaName(), scan.getTableName(), index);
        }

        return new AlignmentOperator(blocks);
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
}
