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
package com.facebook.presto.sql.planner;

import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.optimizations.ImplementSampleAsFilter;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MaterializeSamplePullUp;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.PruneRedundantProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Provider;

import java.util.List;

public class PlanOptimizersFactory
        implements Provider<List<PlanOptimizer>>
{
    private final List<PlanOptimizer> optimizers;

    @Inject
    public PlanOptimizersFactory(Metadata metadata, SplitManager splitManager, IndexManager indexManager, FeaturesConfig featuresConfig)
    {
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new ImplementSampleAsFilter(),
                new CanonicalizeExpressions(),
                new SimplifyExpressions(metadata),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new SetFlatteningOptimizer(),
                new MaterializeSamplePullUp(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PredicatePushDown(metadata, splitManager, featuresConfig.isExperimentalSyntaxEnabled()),
                new PredicatePushDown(metadata, splitManager, featuresConfig.isExperimentalSyntaxEnabled()), // Run predicate push down one more time in case we can leverage new information from generated partitions
                new MergeProjections(),
                new SimplifyExpressions(metadata), // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new UnaliasSymbolReferences(), // Run again because predicate pushdown might add more projections
                new IndexJoinOptimizer(indexManager, metadata), // Run this after projections and filters have been fully simplified and pushed down
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new PruneRedundantProjections()); // This MUST run after PruneUnreferencedOutputs as it may introduce new redundant projections
        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    @Override
    public synchronized List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
