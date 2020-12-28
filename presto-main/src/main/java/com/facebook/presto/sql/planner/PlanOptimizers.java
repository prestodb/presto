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

import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculator.EstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.AddIntermediateAggregations;
import com.facebook.presto.sql.planner.iterative.rule.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.iterative.rule.CreatePartialTopN;
import com.facebook.presto.sql.planner.iterative.rule.DesugarAtTimeZone;
import com.facebook.presto.sql.planner.iterative.rule.DesugarCurrentUser;
import com.facebook.presto.sql.planner.iterative.rule.DesugarLambdaExpression;
import com.facebook.presto.sql.planner.iterative.rule.DesugarRowSubscript;
import com.facebook.presto.sql.planner.iterative.rule.DesugarTryExpression;
import com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroDistinctLimit;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroLimit;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroSample;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroTopN;
import com.facebook.presto.sql.planner.iterative.rule.ExtractSpatialJoins;
import com.facebook.presto.sql.planner.iterative.rule.GatherAndMergeWindows;
import com.facebook.presto.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import com.facebook.presto.sql.planner.iterative.rule.ImplementFilteredAggregations;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.InlineSqlFunctions;
import com.facebook.presto.sql.planner.iterative.rule.MergeFilters;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithDistinct;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithSort;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithTopN;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimits;
import com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PickTableLayout;
import com.facebook.presto.sql.planner.iterative.rule.PlanRemotePojections;
import com.facebook.presto.sql.planner.iterative.rule.PruneAggregationColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneAggregationSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneCountAggregationOverScalar;
import com.facebook.presto.sql.planner.iterative.rule.PruneCrossJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneFilterColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneIndexSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneJoinChildrenColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneLimitColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneMarkDistinctColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneOrderByInAggregation;
import com.facebook.presto.sql.planner.iterative.rule.PruneOutputColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneProjectColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneRedundantProjectionAssignments;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableScanColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTopNColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneValuesColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneWindowColumns;
import com.facebook.presto.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushDownDereferences;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughProject;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import com.facebook.presto.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushTopNThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.RemoveEmptyDelete;
import com.facebook.presto.sql.planner.iterative.rule.RemoveFullSample;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantLimit;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantSort;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantTopN;
import com.facebook.presto.sql.planner.iterative.rule.RemoveTrivialFilters;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins;
import com.facebook.presto.sql.planner.iterative.rule.RewriteFilterWithExternalFunctionToProject;
import com.facebook.presto.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import com.facebook.presto.sql.planner.iterative.rule.RuntimeReorderJoinSides;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyCountOverConstant;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyRowExpressions;
import com.facebook.presto.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import com.facebook.presto.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TranslateExpressions;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.ApplyConnectorOptimization;
import com.facebook.presto.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MetadataDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.PushdownSubfields;
import com.facebook.presto.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
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

import static com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager.PlanPhase.LOGICAL;
import static com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager.PlanPhase.PHYSICAL;

public class PlanOptimizers
{
    private final List<PlanOptimizer> planningTimeOptimizers;
    private final List<PlanOptimizer> runtimeOptimizers;
    private final RuleStatsRecorder ruleStats = new RuleStatsRecorder();
    private final OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
    private final MBeanExporter exporter;

    @Inject
    public PlanOptimizers(
            Metadata metadata,
            SqlParser sqlParser,
            MBeanExporter exporter,
            SplitManager splitManager,
            ConnectorPlanOptimizerManager planOptimizerManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            @EstimatedExchanges CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator)
    {
        this(metadata,
                sqlParser,
                false,
                exporter,
                splitManager,
                planOptimizerManager,
                pageSourceManager,
                statsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                costComparator,
                taskCountEstimator);
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

    public PlanOptimizers(
            Metadata metadata,
            SqlParser sqlParser,
            boolean forceSingleNode,
            MBeanExporter exporter,
            SplitManager splitManager,
            ConnectorPlanOptimizerManager planOptimizerManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator)
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
                new PruneLimitColumns(),
                new PruneTableScanColumns());

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionAndTypeManager()),
                        new RemoveRedundantIdentityProjections()));

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new PushProjectionThroughUnion(),
                        new PushProjectionThroughExchange()));

        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyExpressions(metadata, sqlParser).rules());

        IterativeOptimizer simplifyRowExpressionOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new SimplifyRowExpressions(metadata).rules())
                        .add(new PruneRedundantProjectionAssignments())
                        .build());

        PlanOptimizer predicatePushDown = new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, sqlParser));

        builder.add(
                // Clean up all the sugar in expressions, e.g. AtTimeZone, must be run before all the other optimizers
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new DesugarAtTimeZone(metadata, sqlParser).rules())
                                .addAll(new DesugarCurrentUser().rules())
                                .addAll(new DesugarTryExpression().rules())
                                .addAll(new DesugarRowSubscript(metadata, sqlParser).rules())
                                .build()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new CanonicalizeExpressions().rules()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                .addAll(ImmutableSet.of(
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new EvaluateZeroLimit(),
                                        new EvaluateZeroDistinctLimit(),
                                        new EvaluateZeroTopN(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        new PushLimitThroughSemiJoin(),
                                        new PushLimitThroughUnion(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantLimit(),
                                        new RemoveRedundantSort(),
                                        new RemoveRedundantTopN(),
                                        new RemoveRedundantDistinctLimit(),
                                        new ImplementFilteredAggregations(),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new ImplementBernoulliSampleAsFilter(),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata.getFunctionAndTypeManager()),
                                        new PruneOrderByInAggregation(metadata.getFunctionAndTypeManager()),
                                        new RewriteSpatialPartitioningAggregation(metadata)))
                                .build()),
                simplifyOptimizer,
                new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer(),
                new ImplementIntersectAndExceptAsUnion(metadata.getFunctionAndTypeManager()),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new InlineSqlFunctions(metadata, sqlParser).rules()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata.getFunctionAndTypeManager()))),
                new TransformQuantifiedComparisonApplyToLateralJoin(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarLateralNodes(),
                                new TransformUncorrelatedLateralToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata.getFunctionAndTypeManager()),
                                new TransformCorrelatedLateralJoinToJoin())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata.getFunctionAndTypeManager()), // must be run after PruneUnreferencedOutputs
                                new TransformCorrelatedScalarSubquery(), // must be run after TransformCorrelatedScalarAggregationToJoin
                                new TransformCorrelatedLateralJoinToJoin(),
                                new ImplementFilteredAggregations())),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(metadata.getFunctionAndTypeManager()),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject())),
                new CheckSubqueryNodesAreRewritten());

        // TODO: move this before optimization if possible!!
        // Replace all expressions with row expressions
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                new TranslateExpressions(metadata, sqlParser).rules()));
        // After this point, all planNodes should not contain OriginalExpression

        builder.add(
                predicatePushDown,
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new PickTableLayout(metadata).rules()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(metadata.getFunctionAndTypeManager()))),
                inlineProjections,
                simplifyRowExpressionOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager()), // Run again because predicate pushdown and projection pushdown might add more projections
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new SimplifyCountOverConstant(metadata.getFunctionAndTypeManager()))),
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                // add UnaliasSymbolReferences when it's ported
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(GatherAndMergeWindows.rules())
                                .build()),
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this at the end to help clean the plan for logging/execution and not remove info that other optimizers might need at an earlier point
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())),
                predicatePushDown,
                simplifyRowExpressionOptimizer); // Should always run simplifyOptimizer after predicatePushDown

        builder.add(new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new PushDownDereferences(metadata).rules())
                                .build()),
                    new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new PickTableLayout(metadata).rules()));

        // PlanRemoteProjections only handles RowExpression so this need to run after TranslateExpressions
        // Rules applied after this need to handle locality of ProjectNode properly.
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new RewriteFilterWithExternalFunctionToProject(metadata.getFunctionAndTypeManager()),
                        new PlanRemotePojections(metadata.getFunctionAndTypeManager()))));

        // Pass a supplier so that we pickup connector optimizers that are installed later
        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(LOGICAL)),
                projectionPushDown,
                new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections(), new PruneRedundantProjectionAssignments())),
                new PushdownSubfields(metadata));

        builder.add(predicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown

        builder.add(new MetadataQueryOptimizer(metadata));

        // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())),
                predicatePushDown,
                simplifyRowExpressionOptimizer); // Should always run simplifyOptimizer after predicatePushDown

        builder.add(new IterativeOptimizer(
                // Because ReorderJoins runs only once,
                // PredicatePushDown, PruneUnreferenedOutputpus and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new ReorderJoins(costComparator, metadata))));

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughUnion())));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager).rules())
                        .add(new InlineProjections(metadata.getFunctionAndTypeManager()))
                        .build()));

        if (!forceSingleNode) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges
            builder.add((new IterativeOptimizer(
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator)))));
            builder.add(
                    new IterativeOptimizer(
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(metadata, sqlParser)));
        }

        //noinspection UnusedAssignment
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges

        builder.add(
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveEmptyDelete()))); // Run RemoveEmptyDelete after table scan is removed by PickTableLayout/AddExchanges

        builder.add(predicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new RemoveUnsupportedDynamicFilters(metadata.getFunctionAndTypeManager()));
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown
        builder.add(projectionPushDown);
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager())); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new InlineProjections(metadata.getFunctionAndTypeManager()))
                        .build()));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, sqlParser));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new PushPartialAggregationThroughJoin(),
                        new PushPartialAggregationThroughExchange(metadata.getFunctionAndTypeManager()),
                        new PruneJoinColumns())));

        builder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));

        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(PHYSICAL)),
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections(), new PruneRedundantProjectionAssignments())));

        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata.getFunctionAndTypeManager()));
        builder.add(new MetadataDeleteOptimizer(metadata));

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point
        this.planningTimeOptimizers = builder.build();

        // Add runtime cost-based optimizers
        ImmutableList.Builder<PlanOptimizer> runtimeBuilder = ImmutableList.builder();
        runtimeBuilder.add(new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableList.of(),
                ImmutableSet.of(new RuntimeReorderJoinSides(metadata, sqlParser))));
        this.runtimeOptimizers = runtimeBuilder.build();
    }

    public List<PlanOptimizer> getPlanningTimeOptimizers()
    {
        return planningTimeOptimizers;
    }

    public List<PlanOptimizer> getRuntimeOptimizers()
    {
        return runtimeOptimizers;
    }
}
