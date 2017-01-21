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
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroLimit;
import com.facebook.presto.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithDistinct;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithSort;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithTopN;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimits;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableScanColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneValuesColumns;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughProject;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyCountOverConstant;
import com.facebook.presto.sql.planner.iterative.rule.SingleMarkDistinctToGroupBy;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.BeginTableWrite;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.optimizations.CountConstantOptimizer;
import com.facebook.presto.sql.planner.optimizations.DesugaringOptimizer;
import com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins;
import com.facebook.presto.sql.planner.optimizations.EmptyDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementFilteredAggregations;
import com.facebook.presto.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import com.facebook.presto.sql.planner.optimizations.ImplementSampleAsFilter;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.MergeWindows;
import com.facebook.presto.sql.planner.optimizations.MetadataDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import com.facebook.presto.sql.planner.optimizations.PartialAggregationPushDown;
import com.facebook.presto.sql.planner.optimizations.PickLayout;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.ProjectionPushDown;
import com.facebook.presto.sql.planner.optimizations.PruneIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.PushTableWriteThroughUnion;
import com.facebook.presto.sql.planner.optimizations.RemoveUnreferencedScalarInputApplyNodes;
import com.facebook.presto.sql.planner.optimizations.ReorderWindows;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.SingleDistinctOptimizer;
import com.facebook.presto.sql.planner.optimizations.TransformCorrelatedScalarAggregationToJoin;
import com.facebook.presto.sql.planner.optimizations.TransformExistsApplyToScalarApply;
import com.facebook.presto.sql.planner.optimizations.TransformQuantifiedComparisonApplyToScalarApply;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedScalarToJoin;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;

public class PlanOptimizers
{
    private final List<PlanOptimizer> optimizers;
    private final StatsRecorder stats = new StatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public PlanOptimizers(Metadata metadata, SqlParser sqlParser, FeaturesConfig featuresConfig, MBeanExporter exporter)
    {
        this(metadata, sqlParser, featuresConfig, false, exporter);
    }

    @PostConstruct
    public void initialize()
    {
        stats.export(exporter);
    }

    @PreDestroy
    public void destroy()
    {
        stats.unexport(exporter);
    }

    public PlanOptimizers(Metadata metadata, SqlParser sqlParser, FeaturesConfig featuresConfig, boolean forceSingleNode, MBeanExporter exporter)
    {
        this.exporter = exporter;
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(
                new DesugaringOptimizer(metadata, sqlParser), // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new CanonicalizeExpressions(),
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(
                                new EvaluateZeroLimit(),
                                new PushLimitThroughProject(),
                                new MergeLimits(),
                                new MergeLimitWithSort(),
                                new MergeLimitWithTopN(),
                                new PushLimitThroughMarkDistinct(),
                                new PushLimitThroughSemiJoin(),
                                new MergeLimitWithDistinct(),

                                new PruneValuesColumns(),
                                new PruneTableScanColumns()
                        )),
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(
                                new ImplementFilteredAggregations(),
                                new ImplementSampleAsFilter()),
                        ImmutableSet.of(
                                new com.facebook.presto.sql.planner.iterative.rule.ImplementFilteredAggregations(),
                                new ImplementBernoulliSampleAsFilter())),
                new SimplifyExpressions(metadata, sqlParser),
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new SetFlatteningOptimizer(),
                new ImplementIntersectAndExceptAsUnion(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                new MergeProjections(),
                new TransformExistsApplyToScalarApply(metadata),
                new TransformQuantifiedComparisonApplyToScalarApply(metadata),
                new RemoveUnreferencedScalarInputApplyNodes(),
                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                new TransformUncorrelatedScalarToJoin(),
                new TransformCorrelatedScalarAggregationToJoin(metadata),
                new PredicatePushDown(metadata, sqlParser),
                new MergeProjections(),
                new SimplifyExpressions(metadata, sqlParser), // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new ProjectionPushDown(),
                new UnaliasSymbolReferences(), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(new CountConstantOptimizer()),
                        ImmutableSet.of(new SimplifyCountOverConstant())),
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new MergeWindows(),
                new ReorderWindows(), // Should be after MergeWindows to avoid unnecessary reordering of mergeable windows
                new MergeProjections(),
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new PruneIdentityProjections(), // This MUST run after PruneUnreferencedOutputs as it may introduce new redundant projections
                new MetadataQueryOptimizer(metadata),
                new EliminateCrossJoins(), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                new PredicatePushDown(metadata, sqlParser),
                new ProjectionPushDown());

        if (featuresConfig.isOptimizeSingleDistinct()) {
            builder.add(
                    new IterativeOptimizer(
                            stats,
                            ImmutableList.of(new SingleDistinctOptimizer()),
                            ImmutableSet.of(new SingleMarkDistinctToGroupBy())),
                    new PruneUnreferencedOutputs());
        }

        builder.add(new OptimizeMixedDistinctAggregations(metadata));

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

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new PartialAggregationPushDown(metadata.getFunctionRegistry()));
        builder.add(new PruneIdentityProjections());

        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer());

        builder.add(new MetadataDeleteOptimizer(metadata));
        builder.add(new BeginTableWrite(metadata)); // HACK! see comments in BeginTableWrite

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
