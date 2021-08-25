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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.analyzer.SemanticTypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.enforceSingleRow;

public class EnforceSingleRowStatsRule
        extends SimpleStatsRule<EnforceSingleRowNode>
{
    private static final Pattern<EnforceSingleRowNode> PATTERN = enforceSingleRow();

    public EnforceSingleRowStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<EnforceSingleRowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(EnforceSingleRowNode node, StatsProvider sourceStats, Lookup lookup, Session session, SemanticTypeProvider types)
    {
        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats.getStats(node.getSource()))
                .setOutputRowCount(1)
                .build());
    }
}
