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

package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.OptimizerStatsRecorder;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;

/**
 * Provides the set of PlanOptimizers for use during
 * adaptive query replanning for presto-on-spark.
 */
public class AdaptivePlanOptimizers
{
    private final List<PlanOptimizer> adaptiveOptimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public AdaptivePlanOptimizers(
            MBeanExporter exporter,
            Metadata metadata,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.exporter = exporter;
        this.adaptiveOptimizers = ImmutableList.of(new IterativeOptimizer(ruleStats, statsCalculator, costCalculator, ImmutableSet.of(new PickJoinSides(metadata, sqlParser))));
    }

    @PostConstruct
    public void initialize()
    {
        ruleStats.export(exporter);
        optimizerStats.export(exporter);
    }

    @PreDestroy
    public void destroy()
    {
        ruleStats.unexport(exporter);
        optimizerStats.unexport(exporter);
    }

    public List<PlanOptimizer> getAdaptiveOptimizers()
    {
        return adaptiveOptimizers;
    }
}
