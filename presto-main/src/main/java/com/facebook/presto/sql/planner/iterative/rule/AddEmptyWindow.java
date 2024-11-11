package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.retryQueryWithHistoryBasedOptimizationEnabled;

public class AddEmptyWindow
        implements Rule<OutputNode>
{
    @Override
    public Pattern<OutputNode> getPattern()
    {
        return Pattern.typeOf(OutputNode.class);
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return retryQueryWithHistoryBasedOptimizationEnabled(session);
    }

    @Override
    public Result apply(OutputNode node, Captures captures, Context context)
    {
        if (context.getLookup().resolve(node.getSource()) instanceof WindowNode) {
            return Result.empty();
        }

        WindowNode windowNode = new WindowNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getSource(),
                new WindowNode.Specification(ImmutableList.of(node.getSource().getOutputVariables().get(0)),
                        Optional.of(new OrderingScheme(
                                ImmutableList.of(new Ordering(node.getSource().getOutputVariables().get(1), SortOrder.ASC_NULLS_LAST))
                        ))),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        return Result.ofPlanNode(ChildReplacer.replaceChildren(node, ImmutableList.of(windowNode)));
    }
}
