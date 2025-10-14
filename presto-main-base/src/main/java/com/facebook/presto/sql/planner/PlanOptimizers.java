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
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.AddDistinctForSemiJoinBuild;
import com.facebook.presto.sql.planner.iterative.rule.AddExchangesBelowPartialAggregationOverGroupIdRuleSet;
import com.facebook.presto.sql.planner.iterative.rule.AddIntermediateAggregations;
import com.facebook.presto.sql.planner.iterative.rule.AddNotNullFiltersToJoinNode;
import com.facebook.presto.sql.planner.iterative.rule.CombineApproxPercentileFunctions;
import com.facebook.presto.sql.planner.iterative.rule.CreatePartialTopN;
import com.facebook.presto.sql.planner.iterative.rule.CrossJoinWithArrayContainsToInnerJoin;
import com.facebook.presto.sql.planner.iterative.rule.CrossJoinWithArrayNotContainsToAntiJoin;
import com.facebook.presto.sql.planner.iterative.rule.CrossJoinWithOrFilterToInnerJoin;
import com.facebook.presto.sql.planner.iterative.rule.DesugarLambdaExpression;
import com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.DetermineRemotePartitionedExchangeEncoding;
import com.facebook.presto.sql.planner.iterative.rule.DetermineSemiJoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroLimit;
import com.facebook.presto.sql.planner.iterative.rule.EvaluateZeroSample;
import com.facebook.presto.sql.planner.iterative.rule.ExtractSpatialJoins;
import com.facebook.presto.sql.planner.iterative.rule.ExtractSystemTableFilterRuleSet;
import com.facebook.presto.sql.planner.iterative.rule.GatherAndMergeWindows;
import com.facebook.presto.sql.planner.iterative.rule.ImplementBernoulliSampleAsFilter;
import com.facebook.presto.sql.planner.iterative.rule.ImplementFilteredAggregations;
import com.facebook.presto.sql.planner.iterative.rule.ImplementOffset;
import com.facebook.presto.sql.planner.iterative.rule.ImplementTableFunctionSource;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjectionsOnValues;
import com.facebook.presto.sql.planner.iterative.rule.InlineSqlFunctions;
import com.facebook.presto.sql.planner.iterative.rule.LeftJoinNullFilterToSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.LeftJoinWithArrayContainsToEquiJoinCondition;
import com.facebook.presto.sql.planner.iterative.rule.MaterializedViewRewrite;
import com.facebook.presto.sql.planner.iterative.rule.MergeDuplicateAggregation;
import com.facebook.presto.sql.planner.iterative.rule.MergeFilters;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithDistinct;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithSort;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimitWithTopN;
import com.facebook.presto.sql.planner.iterative.rule.MergeLimits;
import com.facebook.presto.sql.planner.iterative.rule.MinMaxByToWindowFunction;
import com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PickTableLayout;
import com.facebook.presto.sql.planner.iterative.rule.PlanRemoteProjections;
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
import com.facebook.presto.sql.planner.iterative.rule.PruneMergeSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneOrderByInAggregation;
import com.facebook.presto.sql.planner.iterative.rule.PruneOutputColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneProjectColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneRedundantProjectionAssignments;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneSemiJoinFilteringSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableFunctionProcessorColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableFunctionProcessorSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableScanColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneTopNColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneUpdateSourceColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneValuesColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneWindowColumns;
import com.facebook.presto.sql.planner.iterative.rule.PullConstantsAboveGroupBy;
import com.facebook.presto.sql.planner.iterative.rule.PullUpExpressionInLambdaRules;
import com.facebook.presto.sql.planner.iterative.rule.PushAggregationThroughOuterJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushDownDereferences;
import com.facebook.presto.sql.planner.iterative.rule.PushDownFilterExpressionEvaluationThroughCrossJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughOffset;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughOuterJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughProject;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushLimitThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushOffsetThroughProject;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushPartialAggregationThroughJoin;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughExchange;
import com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushRemoteExchangeThroughAssignUniqueId;
import com.facebook.presto.sql.planner.iterative.rule.PushRemoteExchangeThroughGroupId;
import com.facebook.presto.sql.planner.iterative.rule.PushTableWriteThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.PushTopNThroughUnion;
import com.facebook.presto.sql.planner.iterative.rule.RandomizeSourceKeyInSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.RemoveCrossJoinWithConstantInput;
import com.facebook.presto.sql.planner.iterative.rule.RemoveEmptyDelete;
import com.facebook.presto.sql.planner.iterative.rule.RemoveFullSample;
import com.facebook.presto.sql.planner.iterative.rule.RemoveIdentityProjectionsBelowProjection;
import com.facebook.presto.sql.planner.iterative.rule.RemoveMapCastRule;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantAggregateDistinct;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantCastToVarcharInJoinClause;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantDistinct;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantDistinctLimit;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantLimit;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantSort;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantSortColumns;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantTableFunction;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantTopN;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantTopNColumns;
import com.facebook.presto.sql.planner.iterative.rule.RemoveTrivialFilters;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarApplyNodes;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnreferencedScalarLateralNodes;
import com.facebook.presto.sql.planner.iterative.rule.RemoveUnsupportedDynamicFilters;
import com.facebook.presto.sql.planner.iterative.rule.ReorderJoins;
import com.facebook.presto.sql.planner.iterative.rule.ReplaceConditionalApproxDistinct;
import com.facebook.presto.sql.planner.iterative.rule.RewriteAggregationIfToFilter;
import com.facebook.presto.sql.planner.iterative.rule.RewriteCaseExpressionPredicate;
import com.facebook.presto.sql.planner.iterative.rule.RewriteCaseToMap;
import com.facebook.presto.sql.planner.iterative.rule.RewriteConstantArrayContainsToInExpression;
import com.facebook.presto.sql.planner.iterative.rule.RewriteExcludeColumnsFunctionToProjection;
import com.facebook.presto.sql.planner.iterative.rule.RewriteFilterWithExternalFunctionToProject;
import com.facebook.presto.sql.planner.iterative.rule.RewriteSpatialPartitioningAggregation;
import com.facebook.presto.sql.planner.iterative.rule.RewriteTableFunctionToTableScan;
import com.facebook.presto.sql.planner.iterative.rule.RuntimeReorderJoinSides;
import com.facebook.presto.sql.planner.iterative.rule.ScaledWriterRule;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyCardinalityMap;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyCountOverConstant;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyRowExpressions;
import com.facebook.presto.sql.planner.iterative.rule.SimplifySortWithConstantInput;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyTopNWithConstantInput;
import com.facebook.presto.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedInPredicateToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedLateralJoinToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedScalarAggregationToJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedScalarSubquery;
import com.facebook.presto.sql.planner.iterative.rule.TransformCorrelatedSingleRowSubqueryToProject;
import com.facebook.presto.sql.planner.iterative.rule.TransformDistinctInnerJoinToLeftEarlyOutJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformDistinctInnerJoinToRightEarlyOutJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformExistsApplyToLateralNode;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedLateralToJoin;
import com.facebook.presto.sql.planner.optimizations.AddExchanges;
import com.facebook.presto.sql.planner.optimizations.AddExchangesForSingleNodeExecution;
import com.facebook.presto.sql.planner.optimizations.AddLocalExchanges;
import com.facebook.presto.sql.planner.optimizations.ApplyConnectorOptimization;
import com.facebook.presto.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import com.facebook.presto.sql.planner.optimizations.CteProjectionAndPredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.GroupInnerJoinsByConnectorRuleSet;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.optimizations.HistoricalStatisticsEquivalentPlanMarkingOptimizer;
import com.facebook.presto.sql.planner.optimizations.ImplementIntersectAndExceptAsUnion;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.JoinPrefilter;
import com.facebook.presto.sql.planner.optimizations.KeyBasedSampler;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.LogicalCteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MergeJoinForSortedInputOptimizer;
import com.facebook.presto.sql.planner.optimizations.MergePartialAggregationsWithFilter;
import com.facebook.presto.sql.planner.optimizations.MetadataDeleteOptimizer;
import com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer;
import com.facebook.presto.sql.planner.optimizations.OptimizeMixedDistinctAggregations;
import com.facebook.presto.sql.planner.optimizations.PayloadJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.PhysicalCteOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.PrefilterForLimitingAggregation;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.PushdownSubfields;
import com.facebook.presto.sql.planner.optimizations.RandomizeNullKeyInOuterJoin;
import com.facebook.presto.sql.planner.optimizations.RemoveRedundantDistinctAggregation;
import com.facebook.presto.sql.planner.optimizations.ReplaceConstantVariableReferencesWithConstants;
import com.facebook.presto.sql.planner.optimizations.ReplicateSemiJoinInDelete;
import com.facebook.presto.sql.planner.optimizations.RewriteIfOverAggregation;
import com.facebook.presto.sql.planner.optimizations.RewriteWriterTarget;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.ShardJoins;
import com.facebook.presto.sql.planner.optimizations.SimplifyPlanWithEmptyInput;
import com.facebook.presto.sql.planner.optimizations.SortMergeJoinOptimizer;
import com.facebook.presto.sql.planner.optimizations.SortedExchangeRule;
import com.facebook.presto.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.TransformQuantifiedComparisonApplyToLateralJoin;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.MBeanExporter;

import java.util.List;
import java.util.Optional;
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
            TaskCountEstimator taskCountEstimator,
            PartitioningProviderManager partitioningProviderManager,
            FeaturesConfig featuresConfig,
            ExpressionOptimizerManager expressionOptimizerManager,
            TaskManagerConfig taskManagerConfig,
            AccessControl accessControl)
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
                taskCountEstimator,
                partitioningProviderManager,
                featuresConfig,
                expressionOptimizerManager,
                taskManagerConfig,
                accessControl);
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
            boolean noExchange,
            MBeanExporter exporter,
            SplitManager splitManager,
            ConnectorPlanOptimizerManager planOptimizerManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            PartitioningProviderManager partitioningProviderManager,
            FeaturesConfig featuresConfig,
            ExpressionOptimizerManager expressionOptimizerManager,
            TaskManagerConfig taskManagerConfig,
            AccessControl accessControl)
    {
        this.exporter = exporter;
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        Set<Rule<?>> predicatePushDownRules = ImmutableSet.of(
                new MergeFilters(metadata.getFunctionAndTypeManager()));

        // TODO: Once we've migrated handling all the plan node types, replace uses of PruneUnreferencedOutputs with an IterativeOptimizer containing these rules.
        Set<Rule<?>> columnPruningRules = ImmutableSet.of(
                new PruneAggregationColumns(),
                new PruneAggregationSourceColumns(),
                new PruneCrossJoinColumns(),
                new PruneFilterColumns(),
                new PruneIndexSourceColumns(),
                new PruneJoinChildrenColumns(),
                new PruneJoinColumns(),
                new PruneUpdateSourceColumns(),
                new PruneMergeSourceColumns(),
                new PruneMarkDistinctColumns(),
                new PruneOutputColumns(),
                new PruneProjectColumns(),
                new PruneSemiJoinColumns(),
                new PruneSemiJoinFilteringSourceColumns(),
                new PruneTopNColumns(),
                new PruneValuesColumns(),
                new PruneWindowColumns(),
                new PruneLimitColumns(),
                new PruneTableFunctionProcessorColumns(),
                new PruneTableFunctionProcessorSourceColumns(),
                new PruneTableScanColumns());

        builder.add(new LogicalCteOptimizer(metadata));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new MaterializedViewRewrite(metadata, accessControl))));

        IterativeOptimizer inlineProjections = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new InlineProjections(metadata.getFunctionAndTypeManager()),
                        new RemoveRedundantIdentityProjections(),
                        new RemoveIdentityProjectionsBelowProjection()));

        IterativeOptimizer projectionPushDown = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new PushProjectionThroughUnion(),
                        new PushProjectionThroughExchange()));

        IterativeOptimizer simplifyRowExpressionOptimizer = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .addAll(new SimplifyRowExpressions(metadata, expressionOptimizerManager).rules())
                        .add(new PruneRedundantProjectionAssignments())
                        .build());

        IterativeOptimizer caseExpressionPredicateRewriter = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new RewriteCaseExpressionPredicate(metadata.getFunctionAndTypeManager()).rules());

        IterativeOptimizer caseToMapRewriter = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new RewriteCaseToMap(metadata.getFunctionAndTypeManager()).rules());

        IterativeOptimizer containsToInRewriter = new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new RewriteConstantArrayContainsToInExpression(metadata.getFunctionAndTypeManager()).rules());

        PlanOptimizer predicatePushDown = new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, sqlParser, expressionOptimizerManager, featuresConfig.isNativeExecutionEnabled()));
        PlanOptimizer prefilterForLimitingAggregation = new StatsRecordingPlanOptimizer(optimizerStats, new PrefilterForLimitingAggregation(metadata, statsCalculator));

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RewriteTableFunctionToTableScan(metadata))));

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new InlineSqlFunctions(metadata).rules())
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new PullUpExpressionInLambdaRules(metadata.getFunctionAndTypeManager()).rules()) // Run before DesugarLambdaExpression
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new DesugarLambdaExpression().rules())
                                .addAll(new SimplifyCardinalityMap(metadata.getFunctionAndTypeManager()).rules())
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EvaluateZeroLimit())),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(predicatePushDownRules)
                                .addAll(columnPruningRules)
                                .addAll(ImmutableSet.of(
                                        new ImplementTableFunctionSource(metadata),
                                        new MergeDuplicateAggregation(metadata.getFunctionAndTypeManager()),
                                        new RemoveRedundantIdentityProjections(),
                                        new RemoveFullSample(),
                                        new EvaluateZeroSample(),
                                        new PushOffsetThroughProject(),
                                        new PushLimitThroughOffset(),
                                        new PushLimitThroughProject(),
                                        new MergeLimits(),
                                        new MergeLimitWithSort(),
                                        new MergeLimitWithTopN(),
                                        new PushLimitThroughMarkDistinct(),
                                        new PushLimitThroughOuterJoin(),
                                        new PushLimitThroughSemiJoin(),
                                        new PushLimitThroughUnion(),
                                        new RemoveTrivialFilters(),
                                        new RemoveRedundantTableFunction(),
                                        new ImplementFilteredAggregations(metadata.getFunctionAndTypeManager()),
                                        new SingleDistinctAggregationToGroupBy(),
                                        new MultipleDistinctAggregationToMarkDistinct(),
                                        new ImplementBernoulliSampleAsFilter(metadata.getFunctionAndTypeManager()),
                                        new MergeLimitWithDistinct(),
                                        new PruneCountAggregationOverScalar(metadata.getFunctionAndTypeManager()),
                                        new PruneOrderByInAggregation(metadata.getFunctionAndTypeManager()),
                                        new RewriteSpatialPartitioningAggregation(metadata)))
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new ImplementBernoulliSampleAsFilter(metadata.getFunctionAndTypeManager()),
                                new ImplementOffset(metadata.getFunctionAndTypeManager()))),
                simplifyRowExpressionOptimizer,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveTrivialFilters())),
                new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new SetFlatteningOptimizer(),
                new ImplementIntersectAndExceptAsUnion(metadata.getFunctionAndTypeManager()),
                new ReplaceConstantVariableReferencesWithConstants(metadata.getFunctionAndTypeManager()),
                simplifyRowExpressionOptimizer,
                new ReplaceConstantVariableReferencesWithConstants(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new ReplaceConditionalApproxDistinct(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new SimplifySortWithConstantInput(),
                                new SimplifyTopNWithConstantInput(),
                                new PullConstantsAboveGroupBy())),
                // Run inlineProjections and PruneUnreferencedOutputs to simplify the plan after ReplaceConstantVariableReferencesWithConstants optimization
                inlineProjections,
                new PruneUnreferencedOutputs(),
                inlineProjections,
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        columnPruningRules),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new TransformExistsApplyToLateralNode(metadata.getFunctionAndTypeManager()))),
                new TransformQuantifiedComparisonApplyToLateralJoin(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarLateralNodes(),
                                new TransformUncorrelatedLateralToJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin(),
                                new TransformUncorrelatedInPredicateSubqueryToSemiJoin(),
                                new TransformCorrelatedScalarAggregationToJoin(metadata.getFunctionAndTypeManager()),
                                new TransformCorrelatedLateralJoinToJoin(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .add(new InlineProjectionsOnValues(metadata.getFunctionAndTypeManager()))
                                .addAll(new SimplifyRowExpressions(metadata, expressionOptimizerManager).rules())
                                .build()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new RemoveUnreferencedScalarApplyNodes(),
                                new TransformCorrelatedInPredicateToJoin(metadata.getFunctionAndTypeManager()), // must be run after PruneUnreferencedOutputs
                                new TransformCorrelatedScalarSubquery(metadata.getFunctionAndTypeManager()), // must be run after TransformCorrelatedScalarAggregationToJoin
                                new TransformCorrelatedLateralJoinToJoin(metadata.getFunctionAndTypeManager()),
                                new ImplementFilteredAggregations(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new InlineProjections(metadata.getFunctionAndTypeManager()),
                                new RemoveRedundantIdentityProjections(),
                                new TransformCorrelatedSingleRowSubqueryToProject())),
                new CheckSubqueryNodesAreRewritten(),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder().add(new RemoveRedundantCastToVarcharInJoinClause(metadata.getFunctionAndTypeManager()))
                                .addAll(new RemoveMapCastRule(metadata.getFunctionAndTypeManager()).rules()).build()));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new RemoveCrossJoinWithConstantInput(metadata.getFunctionAndTypeManager()))));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new CombineApproxPercentileFunctions(metadata.getFunctionAndTypeManager()))));

        // In RewriteIfOverAggregation, we can only optimize when the aggregation output is used in only one IF expression, and not used in any other expressions (excluding
        // identity assignments). Hence we need to simplify projection assignments to combine/inline expressions in assignments so as to identify the candidate IF expressions.
        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new PruneRedundantProjectionAssignments(),
                                new InlineProjections(metadata.getFunctionAndTypeManager()),
                                new RemoveRedundantIdentityProjections())),
                new RewriteIfOverAggregation(metadata.getFunctionAndTypeManager()));

        builder.add(
                caseToMapRewriter,
                caseExpressionPredicateRewriter,
                containsToInRewriter,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RewriteAggregationIfToFilter(metadata.getFunctionAndTypeManager()))),
                predicatePushDown,
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new PushDownFilterExpressionEvaluationThroughCrossJoin(metadata.getFunctionAndTypeManager()),
                                new CrossJoinWithOrFilterToInnerJoin(metadata.getFunctionAndTypeManager()),
                                new CrossJoinWithArrayContainsToInnerJoin(metadata.getFunctionAndTypeManager()),
                                new CrossJoinWithArrayNotContainsToAntiJoin(metadata, metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new LeftJoinWithArrayContainsToEquiJoinCondition(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new LeftJoinNullFilterToSemiJoin(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new AddDistinctForSemiJoinBuild())),
                new KeyBasedSampler(metadata),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new PickTableLayout(metadata).rules()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        Optional.of(new LogicalPropertiesProviderImpl(new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()))),
                        ImmutableSet.of(
                                new RemoveRedundantDistinct(),
                                new RemoveRedundantTopN(),
                                new RemoveRedundantTopNColumns(),
                                new RemoveRedundantSort(),
                                new RemoveRedundantSortColumns(),
                                new RemoveRedundantLimit(),
                                new RemoveRedundantDistinctLimit(),
                                new RemoveRedundantAggregateDistinct(),
                                new RemoveRedundantIdentityProjections(),
                                new PushAggregationThroughOuterJoin(metadata.getFunctionAndTypeManager()))),
                inlineProjections,
                simplifyRowExpressionOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                projectionPushDown,
                new PayloadJoinOptimizer(metadata),
                new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager()), // Run again because predicate pushdown and projection pushdown might add more projections
                // At this time, we may have some new constant variables after the first run of this optimizer, hence rerun here
                new ReplaceConstantVariableReferencesWithConstants(metadata.getFunctionAndTypeManager()),
                simplifyRowExpressionOptimizer,
                new ReplaceConstantVariableReferencesWithConstants(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(
                                new SimplifySortWithConstantInput(),
                                new SimplifyTopNWithConstantInput(),
                                new PullConstantsAboveGroupBy())),
                // Run inlineProjections and PruneUnreferencedOutputs to simplify the plan after ReplaceConstantVariableReferencesWithConstants optimization
                inlineProjections,
                new PruneUnreferencedOutputs(), // Make sure to run this before index join. Filtered projections may not have all the columns.
                new IndexJoinOptimizer(metadata), // Run this after projections and filters have been fully simplified and pushed down
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        Optional.of(new LogicalPropertiesProviderImpl(new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()))),
                        ImmutableSet.of(
                                new AddNotNullFiltersToJoinNode(metadata.getFunctionAndTypeManager()))), // run this optimizer after IndexJoinOptimizer as it may add filters to outer join nodes incompatible with index scan
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new SimplifyCountOverConstant(metadata.getFunctionAndTypeManager()))),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new MinMaxByToWindowFunction(metadata.getFunctionAndTypeManager()))),
                new LimitPushDown(), // Run LimitPushDown before WindowFilterPushDown
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                prefilterForLimitingAggregation,
                new IterativeOptimizer(
                        metadata,
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
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new InlineSqlFunctions(metadata).rules())
                                .build()));

        builder.add(new JoinPrefilter(metadata));

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())),
                predicatePushDown,
                simplifyRowExpressionOptimizer); // Should always run simplifyOptimizer after predicatePushDown

        builder.add(new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new PushDownDereferences(metadata).rules())
                                .build()),
                new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new PickTableLayout(metadata).rules()));

        // PlanRemoteProjections only handles RowExpression so this need to run after TranslateExpressions
        // Rules applied after this need to handle locality of ProjectNode properly.
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new RewriteFilterWithExternalFunctionToProject(metadata.getFunctionAndTypeManager()),
                        new PlanRemoteProjections(metadata.getFunctionAndTypeManager()))));

        // Pass a supplier so that we pickup connector optimizers that are installed later
        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(LOGICAL)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new RewriteFilterWithExternalFunctionToProject(metadata.getFunctionAndTypeManager()),
                                new PlanRemoteProjections(metadata.getFunctionAndTypeManager()))),
                projectionPushDown,
                new PruneUnreferencedOutputs());

        // Pass after connector optimizer, as it relies on connector optimizer to identify empty input tables and convert them to empty ValuesNode
        builder.add(new SimplifyPlanWithEmptyInput(),
                new PruneUnreferencedOutputs());

        builder.add(new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections(), new PruneRedundantProjectionAssignments(), new RemoveRedundantTableFunction())),
                new PushdownSubfields(metadata, expressionOptimizerManager));

        builder.add(predicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown

        builder.add(new MetadataQueryOptimizer(metadata, expressionOptimizerManager));

        // This can pull up Filter and Project nodes from between Joins, so we need to push them down again
        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        ImmutableSet.of(new EliminateCrossJoins())),
                predicatePushDown,
                simplifyRowExpressionOptimizer); // Should always run simplifyOptimizer after predicatePushDown

        builder.add(new OptimizeMixedDistinctAggregations(metadata));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(
                        new CreatePartialTopN(),
                        new PushTopNThroughUnion())));

        // We do a single pass, and assign `statsEquivalentPlanNode` to each node.
        // After this step, nodes with same `statsEquivalentPlanNode` will share same history based statistics.
        builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new HistoricalStatisticsEquivalentPlanMarkingOptimizer(statsCalculator)));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new GroupInnerJoinsByConnectorRuleSet(metadata, predicatePushDown).rules()));

        builder.add(new IterativeOptimizer(
                metadata,
                // Because ReorderJoins runs only once,
                // PredicatePushDown, PruneUnreferencedOutputs and RemoveRedundantIdentityProjections
                // need to run beforehand in order to produce an optimal join order
                // It also needs to run after EliminateCrossJoins so that its chosen order doesn't get undone.
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                ImmutableSet.of(new ReorderJoins(costComparator, metadata))));

        // After ReorderJoins, `statsEquivalentPlanNode` will be unassigned to intermediate join nodes.
        // We run it again to mark this for intermediate join nodes.
        builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new HistoricalStatisticsEquivalentPlanMarkingOptimizer(statsCalculator)));

        // Run this set of join transformations after ReorderJoins, but before DetermineJoinDistributionType
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                Optional.of(new LogicalPropertiesProviderImpl(new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()))),
                ImmutableSet.of(
                        new TransformDistinctInnerJoinToLeftEarlyOutJoin(),
                        new TransformDistinctInnerJoinToRightEarlyOutJoin(),
                        new RemoveRedundantDistinct(),
                        new RemoveRedundantTopN(),
                        new RemoveRedundantSort(),
                        new RemoveRedundantLimit(),
                        new RemoveRedundantDistinctLimit(),
                        new RemoveRedundantAggregateDistinct(),
                        new RemoveRedundantIdentityProjections(),
                        new PushAggregationThroughOuterJoin(metadata.getFunctionAndTypeManager()))));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .addAll(new ExtractSpatialJoins(metadata, splitManager, pageSourceManager).rules())
                        .add(new InlineProjections(metadata.getFunctionAndTypeManager()))
                        .build()));

        builder.add(new RemoveRedundantDistinctAggregation());

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new ScaledWriterRule())));

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RewriteTableFunctionToTableScan(metadata), new RewriteExcludeColumnsFunctionToProjection())));

        if (!noExchange) {
            builder.add(new ReplicateSemiJoinInDelete()); // Must run before AddExchanges

            builder.add(new IterativeOptimizer(
                    metadata,
                    ruleStats,
                    statsCalculator,
                    estimatedExchangesCostCalculator,
                    ImmutableSet.of(
                            new DetermineJoinDistributionType(costComparator, taskCountEstimator), // Must run before AddExchanges
                            // Must run before AddExchanges and after ReplicateSemiJoinInDelete
                            // to avoid temporarily having an invalid plan
                            new DetermineSemiJoinDistributionType(costComparator, taskCountEstimator))));

            builder.add(
                    new IterativeOptimizer(
                            metadata,
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(
                                    new RandomizeSourceKeyInSemiJoin(metadata.getFunctionAndTypeManager()))),
                    new RandomizeNullKeyInOuterJoin(metadata.getFunctionAndTypeManager(), statsCalculator),
                    new PruneUnreferencedOutputs(),
                    new IterativeOptimizer(
                            metadata,
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(
                                    new PruneRedundantProjectionAssignments(),
                                    new InlineProjections(metadata.getFunctionAndTypeManager()),
                                    new RemoveRedundantIdentityProjections())));

            builder.add(new ShardJoins(metadata, metadata.getFunctionAndTypeManager(), statsCalculator),
                    new PruneUnreferencedOutputs());
            builder.add(
                    new IterativeOptimizer(
                            metadata,
                            ruleStats,
                            statsCalculator,
                            estimatedExchangesCostCalculator,
                            ImmutableSet.of(new PushTableWriteThroughUnion()))); // Must run before AddExchanges
            builder.add(new CteProjectionAndPredicatePushDown(metadata, expressionOptimizerManager)); // must run before PhysicalCteOptimizer
            builder.add(new PhysicalCteOptimizer(metadata)); // Must run before AddExchanges
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchanges(metadata, partitioningProviderManager, featuresConfig.isNativeExecutionEnabled())));
            builder.add(new StatsRecordingPlanOptimizer(optimizerStats, new AddExchangesForSingleNodeExecution(metadata)));
        }

        //noinspection UnusedAssignment
        estimatedExchangesCostCalculator = null; // Prevent accidental use after AddExchanges

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        // Run RemoveEmptyDelete and EliminateEmptyJoins after table scan is removed by PickTableLayout/AddExchanges
                        ImmutableSet.of(new RemoveEmptyDelete())));
        builder.add(predicatePushDown); // Run predicate push down one more time in case we can leverage new information from layouts' effective predicate
        builder.add(new RemoveUnsupportedDynamicFilters(metadata.getFunctionAndTypeManager()));
        builder.add(simplifyRowExpressionOptimizer); // Should be always run after PredicatePushDown
        builder.add(projectionPushDown);
        builder.add(inlineProjections);
        builder.add(new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager())); // Run unalias after merging projections to simplify projections more efficiently
        builder.add(new PruneUnreferencedOutputs());
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.<Rule<?>>builder()
                        .add(new RemoveRedundantIdentityProjections())
                        .add(new PushRemoteExchangeThroughAssignUniqueId())
                        .add(new PushRemoteExchangeThroughGroupId(metadata))
                        .add(new InlineProjections(metadata.getFunctionAndTypeManager()))
                        .build()));

        // MergeJoinForSortedInputOptimizer can avoid the local exchange for a join operation
        // Should be placed after AddExchanges, but before AddLocalExchange
        // To replace the JoinNode to MergeJoin ahead of AddLocalExchange to avoid adding extra local exchange
        builder.add(new MergeJoinForSortedInputOptimizer(metadata, featuresConfig.isNativeExecutionEnabled(), featuresConfig.isPrestoSparkExecutionEnvironment()),
                new SortMergeJoinOptimizer(metadata, featuresConfig.isNativeExecutionEnabled()));
        // SortedExchangeRule pushes sorts down to exchange nodes for distributed queries
        // The rule is added unconditionally but only applies when:
        // 1. Native execution is enabled
        // 2. Running in Presto Spark execution environment
        // 3. Session property sorted_exchange_enabled is true
        builder.add(new SortedExchangeRule(
                featuresConfig.isNativeExecutionEnabled() && featuresConfig.isPrestoSparkExecutionEnvironment()));

        // Optimizers above this don't understand local exchanges, so be careful moving this.
        builder.add(new AddLocalExchanges(metadata, featuresConfig.isNativeExecutionEnabled()));

        // Optimizers above this do not need to care about aggregations with the type other than SINGLE
        // This optimizer must be run after all exchange-related optimizers
        builder.add(new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new PushPartialAggregationThroughJoin(),
                                new PushPartialAggregationThroughExchange(metadata.getFunctionAndTypeManager(), featuresConfig.isNativeExecutionEnabled()))),
                // MergePartialAggregationsWithFilter should immediately follow PushPartialAggregationThroughExchange
                new MergePartialAggregationsWithFilter(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(
                                new PruneJoinColumns())));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(taskCountEstimator, taskManagerConfig, metadata, featuresConfig.isNativeExecutionEnabled()).rules()));

        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(
                        new AddIntermediateAggregations(),
                        new RemoveRedundantIdentityProjections())));

        builder.add(
                new ApplyConnectorOptimization(() -> planOptimizerManager.getOptimizers(PHYSICAL)),
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        ImmutableSet.of(new RemoveRedundantIdentityProjections(), new PruneRedundantProjectionAssignments())));

        // Pass after connector optimizer, as it relies on connector optimizer to identify empty input tables and convert them to empty ValuesNode
        builder.add(new SimplifyPlanWithEmptyInput());

        builder.add(
                new IterativeOptimizer(
                        metadata,
                        ruleStats,
                        statsCalculator,
                        costCalculator,
                        new ExtractSystemTableFilterRuleSet(metadata.getFunctionAndTypeManager()).rules()));

        // DO NOT add optimizers that change the plan shape (computations) after this point

        // Precomputed hashes - this assumes that partitioning will not change
        builder.add(new HashGenerationOptimizer(metadata.getFunctionAndTypeManager()));
        builder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableSet.of(new DetermineRemotePartitionedExchangeEncoding(
                        featuresConfig.isNativeExecutionEnabled(),
                        featuresConfig.isPrestoSparkExecutionEnvironment()))));
        builder.add(new MetadataDeleteOptimizer(metadata));

        builder.add(new RewriteWriterTarget());

        // TODO: consider adding a formal final plan sanitization optimizer that prepares the plan for transmission/execution/logging
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point
        this.planningTimeOptimizers = builder.build();

        // Add runtime cost-based optimizers
        ImmutableList.Builder<PlanOptimizer> runtimeBuilder = ImmutableList.builder();
        runtimeBuilder.add(new IterativeOptimizer(
                metadata,
                ruleStats,
                statsCalculator,
                costCalculator,
                ImmutableList.of(),
                ImmutableSet.of(new RuntimeReorderJoinSides(metadata, featuresConfig.isNativeExecutionEnabled()))));
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
