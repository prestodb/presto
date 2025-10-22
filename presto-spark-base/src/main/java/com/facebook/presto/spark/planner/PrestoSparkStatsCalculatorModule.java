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

package com.facebook.presto.spark.planner;

import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.FragmentStatsProvider;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.cost.StatsCalculatorModule.createComposableStatsCalculator;

public class PrestoSparkStatsCalculatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ScalarStatsCalculator.class).in(Scopes.SINGLETON);
        binder.bind(StatsNormalizer.class).in(Scopes.SINGLETON);
        binder.bind(FilterStatsCalculator.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HistoryBasedOptimizationConfig.class);
        binder.bind(HistoryBasedPlanStatisticsManager.class).in(Scopes.SINGLETON);
        binder.bind(FragmentStatsProvider.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static StatsCalculator createNewStatsCalculator(
            Metadata metadata,
            ScalarStatsCalculator scalarStatsCalculator,
            StatsNormalizer normalizer,
            FilterStatsCalculator filterStatsCalculator,
            FragmentStatsProvider fragmentStatsProvider,
            HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager,
            HistoryBasedOptimizationConfig historyBasedOptimizationConfig,
            ExpressionOptimizerManager expressionOptimizerManager)
    {
        StatsCalculator delegate = createComposableStatsCalculator(metadata, scalarStatsCalculator, normalizer, filterStatsCalculator, fragmentStatsProvider, expressionOptimizerManager);
        return new PrestoSparkStatsCalculator(historyBasedPlanStatisticsManager.getHistoryBasedPlanStatisticsCalculator(delegate), delegate, historyBasedOptimizationConfig);
    }
}
