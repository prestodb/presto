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
import com.google.inject.Scopes;

import javax.inject.Singleton;

public class StatsCalculatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ScalarStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(StatsNormalizer.class).in(Scopes.SINGLETON);
        binder.bind(FilterStatsCalculator.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static StatsCalculator createNewStatsCalculator(
            Metadata metadata,
            ScalarStatsCalculator scalarStatsCalculator,
            StatsNormalizer normalizer,
            FilterStatsCalculator filterStatsCalculator)
    {
        ImmutableList.Builder<ComposableStatsCalculator.Rule<?>> rules = ImmutableList.builder();
        rules.add(new OutputStatsRule());
        rules.add(new TableScanStatsRule(metadata, normalizer));
        rules.add(new SimpleFilterProjectSemiJoinStatsRule(normalizer, filterStatsCalculator, metadata.getFunctionAndTypeManager())); // this must be before FilterStatsRule
        rules.add(new FilterStatsRule(normalizer, filterStatsCalculator));
        rules.add(new ValuesStatsRule(metadata));
        rules.add(new LimitStatsRule(normalizer));
        rules.add(new EnforceSingleRowStatsRule(normalizer));
        rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
        rules.add(new ExchangeStatsRule(normalizer));
        rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
        rules.add(new SpatialJoinStatsRule(filterStatsCalculator, normalizer));
        rules.add(new AggregationStatsRule(normalizer));
        rules.add(new UnionStatsRule(normalizer));
        rules.add(new AssignUniqueIdStatsRule());
        rules.add(new SemiJoinStatsRule());
        rules.add(new RowNumberStatsRule(normalizer));
        rules.add(new UnnestStatsRule());
        rules.add(new SortStatsRule());
        rules.add(new SampleStatsRule(normalizer));
        rules.add(new IntersectStatsRule(normalizer));

        return new ComposableStatsCalculator(rules.build());
    }
}
