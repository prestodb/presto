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

import com.facebook.presto.metadata.Metadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;

import javax.inject.Singleton;

public class StatsCalculatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    @Singleton
    public static StatsCalculator createNewStatsCalculator(Metadata metadata)
    {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(metadata);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(metadata, scalarStatsCalculator, normalizer);

        ImmutableList.Builder<ComposableStatsCalculator.Rule<?>> rules = ImmutableList.builder();
        rules.add(new OutputStatsRule());
        rules.add(new TableScanStatsRule(metadata, normalizer));
        rules.add(new SimpleFilterProjectSemiJoinStatsRule(normalizer, filterStatsCalculator)); // this must be before FilterStatsRule
        rules.add(new FilterStatsRule(filterStatsCalculator));
        rules.add(new ValuesStatsRule(metadata));
        rules.add(new LimitStatsRule(normalizer));
        rules.add(new EnforceSingleRowStatsRule(normalizer));
        rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
        rules.add(new ExchangeStatsRule(normalizer));
        rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
        rules.add(new AggregationStatsRule(normalizer));
        rules.add(new UnionStatsRule(normalizer));
        rules.add(new AssignUniqueIdStatsRule());
        rules.add(new SemiJoinStatsRule());

        return new ComposableStatsCalculator(rules.build());
    }
}
