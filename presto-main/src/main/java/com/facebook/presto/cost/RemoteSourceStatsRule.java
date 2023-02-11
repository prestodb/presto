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
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.remoteSource;
import static java.util.Objects.requireNonNull;

public class RemoteSourceStatsRule
        extends SimpleStatsRule<RemoteSourceNode>
{
    private static final Pattern<RemoteSourceNode> PATTERN = remoteSource();

    private final FragmentStatsProvider fragmentStatsProvider;

    public RemoteSourceStatsRule(FragmentStatsProvider fragmentStatsProvider, StatsNormalizer normalizer)
    {
        super(normalizer);
        this.fragmentStatsProvider = requireNonNull(fragmentStatsProvider, "metadata is null");
    }

    @Override
    public Pattern<RemoteSourceNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(RemoteSourceNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        QueryId queryId = session.getQueryId();
        return node.getSourceFragmentIds().stream()
                .map(fragmentId -> fragmentStatsProvider.getStats(queryId, fragmentId))
                .reduce(PlanNodeStatsEstimateMath::addStatsAndCollapseDistinctValues);
    }
}
