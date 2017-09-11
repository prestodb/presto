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
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.AddIntermediateAggregations;
import com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.iterative.rule.CreatePartialTopN;
import com.facebook.presto.sql.planner.iterative.rule.DesugarAtTimeZone;
import com.facebook.presto.sql.planner.iterative.rule.DesugarLambdaExpression;
import com.facebook.presto.sql.planner.iterative.rule.DesugarTryExpression;
import com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroLimit;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroSample;
import com.facebook.presto.sql.planner.iterative.rule.GatherAndMergeWindows;
import com.facebook.presto.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import com.facebook.presto.sql.planner.iterative.rule.ImplementFilteredAggregations;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.MergeFilters;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithDistinct;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithSort;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithTopN;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimits;
import com.facebook.presto.sql.planner.iterative.rule.PickTableLayout;
import com.facebook.presto.sql.planner.iterative.rule.PruneAggregationColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import com.facebook.presto.sql.planner.iterative.rule.PruneCrossJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneFilterColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneIndexSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneOutputColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneProjectColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableScanColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTopNColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneValuesColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneWindowColumns;
import com.facebook.presto.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughProject;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushTopNThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.RemoveEmptyDelete;
import com.facebook.presto.sql.planner.iterative.rule.RemoveFullSample;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.RemoveTrivialFilters;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyCountOverConstant;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions;
import com.facebook.presto.sql.planner.iterative.rule.SingleMarkDistinctToGroupBy;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.BeginTableWrite;
import com.facebook.presto.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import com.facebook.presto.sql.planner.optimizations.DesugaringOptimizer;
import com.facebook.presto.sql.planner.optimizations.DetermineJoinDistributionType;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MetadataDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.ProjectionPushDown;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.RemoveUnreferencedScalarLateralNodes;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.TransformCorrelatedNoAggregationSubqueryToJoin;
import com.facebook.presto.sql.planner.optimizations.TransformCorrelatedScalarAggregationToJoin;
import com.facebook.presto.sql.planner.optimizations.TransformCorrelatedSingleRowSubqueryToProject;
import com.facebook.presto.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.optimizations.TransformUncorrelatedLateralToJoin;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;

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

        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters());

        // TODO: Once we've migrated handling all the plan node types, replace uses of PruneUnreferencedOutputs with an IterativeOptimizer containing these rules.
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneCrossJoinColumns(),
                new PruneFilterColumns(),
                new PruneIndexSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOutputColumns(),
                new PruneProjectColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneTopNColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns(),
                new PruneTableScanColumns());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                stats,
                ImmutableSet.of(
                        new InlineProjections(),
                        new RemoveRedundantIdentityProjections()));

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                stats,
                ImmutableList.of(new ProjectionPushDown()),
                ImmutableSet.of(
                        new PushProjectionThroughUnion(),
                        new PushProjectionThroughExchange()));

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                stats,
                ImmutableList.of(new com.facebook.presto.sql.planner.optimizations.SimplifyExpressions(metadata, sqlParser)),
                new SimplifyExpressions(metadata, sqlParser).rules());

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(new DesugaringOptimizer(metadata, sqlParser)),
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, sqlParser).rules())
                                .addAll(new DesugarTryExpression().rules())
                                .build()),
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(new com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions()),
                        new CanonicalizeExpressions().rules()),
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroLimit(),
                                        new EvaluateZeroSample(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughSemiJoin(),
                                        new RemoveTrivialFilters(),
                                        new ImplementFilteredAggregations(),
                                        new ImplementBernoulliSampleAsFilter(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar()))
                                .build()),
                simplifyOptimizer,
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer(),
                new ImplementIntersectAndExceptAsUnion(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata.getFunctionRegistry()))),
                new TransformQuantifiedComparisonApplyToLateralJoin(metadata),
                new IterativeOptimizer(stats,
                        ImmutableList.of(
                                new RemoveUnreferencedScalarLateralNodes(),
                                new TransformUncorrelatedLateralToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata.getFunctionRegistry())),
                        ImmutableSet.of(
                                new com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes(),
                                new com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin(),
                                new com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin(metadata.getFunctionRegistry()))),
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(), // must be run after PruneUnreferencedOutputs
                                new ImplementFilteredAggregations())),
                new TransformCorrelatedNoAggregationSubqueryToJoin(),
                new TransformCorrelatedSingleRowSubqueryToProject(),
                new CheckSubqueryNodesAreRewritten(),
                new PredicatePushDown(metadata, sqlParser),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin())),
                inlineProjections,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                new UnaliasSymbolReferences(), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(new SimplifyCountOverConstant())),
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .build()),
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new MetadataQueryOptimizer(metadata),
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(new com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins()), // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
                        ImmutableSet.of(new EliminateCrossJoins())),
                new PredicatePushDown(metadata, sqlParser),
                projectionPushDown);

        if (featuresConfig.isOptimizeSingleDistinct()) {
            builder.add(
                    new IterativeOptimizer(
                            stats,
                            ImmutableSet.of(new SingleMarkDistinctToGroupBy())),
                    new PruneUnreferencedOutputs());
        }

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                stats,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughUnion())));

        if (!forceSingleNode) {
            builder.add(new DetermineJoinDistributionType()); // Must run before AddExchanges
            builder.add(
                    new IterativeOptimizer(
                            stats,
                            ImmutableList.of(new com.facebook.presto.sql.planner.optimizations.PushTableWriteThroughUnion()), // Must run before AddExchanges
                            ImmutableSet.of(new PushTableWriteThroughUnion())));
            builder.add(new AddExchanges(metadata, sqlParser));
        }

        builder.add(
                new IterativeOptimizer(
                        stats,
                        ImmutableList.of(new com.facebook.presto.sql.planner.optimizations.PickLayout(metadata)),
                        new PickTableLayout(metadata).rules()));

        builder.add(
                new IterativeOptimizer(
                        stats,
                        ImmutableSet.of(new RemoveEmptyDelete()))); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        builder.add(new PredicatePushDown(metadata, sqlParser)); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(projectionPushDown);
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences()); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());
        builder.add(new IterativeOptimizer(
                stats,
                ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, sqlParser));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                stats,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata.getFunctionRegistry()),
                        new PruneJoinColumns())));
        builder.add(new IterativeOptimizer(
                stats,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));
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
