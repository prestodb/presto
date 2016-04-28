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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.BeginTableWrite;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.optimizations.CountConstantOptimizer;
import com.facebook.presto.sql.planner.optimizations.DesugaringOptimizer;
import com.facebook.presto.sql.planner.optimizations.EmptyDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementIntersectAsUnion;
import com.facebook.presto.sql.planner.optimizations.ImplementSampleAsFilter;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.MetadataDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.PickLayout;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.ProjectionPushDown;
import com.facebook.presto.sql.planner.optimizations.PruneIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.PushTableWriteThroughUnion;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.SingleDistinctOptimizer;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedScalarToJoin;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Provider;

import java.util.List;

public class PlanOptimizersFactory
        implements Provider<List<PlanOptimizer>>
{
    private final List<PlanOptimizer> optimizers;

    @Inject
    public PlanOptimizersFactory(Metadata metadata, SqlParser sqlParser, FeaturesConfig featuresConfig)
    {
        this(metadata, sqlParser, featuresConfig, false);
    }

    public PlanOptimizersFactory(Metadata metadata, SqlParser sqlParser, FeaturesConfig featuresConfig, boolean forceSingleNode)
    {
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new DesugaringOptimizer(metadata, sqlParser), // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new TransformUncorrelatedScalarToJoin(),
                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                new ImplementSampleAsFilter(),
                new CanonicalizeExpressions(),
                new SimplifyExpressions(metadata, sqlParser),
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new SetFlatteningOptimizer(),
                new ImplementIntersectAsUnion(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PredicatePushDown(metadata, sqlParser),
                new MergeProjections(),
                new SimplifyExpressions(metadata, sqlParser), // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new ProjectionPushDown(),
                new UnaliasSymbolReferences(), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new CountConstantOptimizer(),
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new MergeProjections(),
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new PruneIdentityProjections(), // This MUST run after PruneUnreferencedOutputs as it may introduce new redundant projections
                new MetadataQueryOptimizer(metadata));

        if (featuresConfig.isOptimizeSingleDistinct()) {
            builder.add(new SingleDistinctOptimizer());
            builder.add(new PruneUnreferencedOutputs());
        }

        if (!forceSingleNode) {
            builder.add(new PushTableWriteThroughUnion()); // Must run before AddExchanges
            builder.add(new AddExchanges(metadata, sqlParser));
        }

        builder.add(new PickLayout(metadata));

        builder.add(new EmptyDeleteOptimizer()); // Run after table scan is removed by PickLayout

        builder.add(new PredicatePushDown(metadata, sqlParser)); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new ProjectionPushDown());
        builder.add(new MergeProjections());
        builder.add(new UnaliasSymbolReferences()); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());
        builder.add(new PruneIdentityProjections());

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, sqlParser));

        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer());

        builder.add(new MetadataDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

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
