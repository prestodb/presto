package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Patterns;

import static com.facebook.presto.SystemSessionProperties.isTransformTopNToStreamingMergeLimit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;
import static java.util.Objects.requireNonNull;

public class TransformTopNToStreamingMergeLimit
    implements Rule<TopNNode>
{
    private static final Capture<TopNNode> CHILD = Capture.newCapture();
    private final int maxTopNCountValue = 10000;
    private static final Pattern<TopNNode> PATTERN = topN()
            .with(Patterns.TopN.step().equalTo(TopNNode.Step.FINAL))
            .with(source().matching(topN().capturedAs(CHILD)));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isTransformTopNToStreamingMergeLimit(session);
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(node.getSource());

        TopNNode child = captures.get(CHILD);

        if (node.getCount() > maxTopNCountValue) {
            return Result.ofPlanNode(new LimitNode(
                    node.getSourceLocation(),
                    node.getId(),
                    child,
                    node.getCount(),
                    LimitNode.Step.FINAL));
        }
        return Result.empty();
    }
}
