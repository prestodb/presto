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

import com.facebook.presto.Session;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.Output;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.PrestoSparkPhysicalResourceCalculator;
import com.facebook.presto.spark.PrestoSparkSourceStatsCollector;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.prestospark.PhysicalResourceSettings;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer.BuiltInPreparedQuery;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CanonicalPlanWithInfo;
import com.facebook.presto.sql.planner.InputExtractor;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.OutputExtractor;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.SparkContext;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.isLogInvokedFunctionNamesEnabled;
import static com.facebook.presto.common.RuntimeMetricName.LOGICAL_PLANNER_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.OPTIMIZER_TIME_NANOS;
import static com.facebook.presto.spark.PrestoSparkSettingsRequirements.SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.analyzer.utils.StatementUtils.getQueryType;
import static com.facebook.presto.sql.planner.PlanNodeCanonicalInfo.getCanonicalInfo;
import static java.util.Objects.requireNonNull;

public class PrestoSparkQueryPlanner
{
    private final SqlParser sqlParser;
    private final PlanOptimizers optimizers;
    private final QueryExplainer queryExplainer;
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final AccessControl accessControl;
    private final PlanChecker planChecker;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;

    @Inject
    public PrestoSparkQueryPlanner(
            SqlParser sqlParser,
            PlanOptimizers optimizers,
            QueryExplainer queryExplainer,
            Metadata metadata,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            AccessControl accessControl,
            PlanChecker planChecker,
            HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.optimizers = requireNonNull(optimizers, "optimizers is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.planCanonicalInfoProvider = requireNonNull(historyBasedPlanStatisticsManager, "historyBasedPlanStatisticsManager is null").getPlanCanonicalInfoProvider();
    }

    public PlanAndMore createQueryPlan(Session session, BuiltInPreparedQuery preparedQuery, WarningCollector warningCollector, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, SparkContext sparkContext)
    {
        Analyzer analyzer = new Analyzer(
                session,
                metadata,
                sqlParser,
                accessControl,
                Optional.of(queryExplainer),
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                session,
                idAllocator,
                metadata,
                variableAllocator,
                sqlParser);

        PlanNode planNode = session.getRuntimeStats().profileNanos(
                LOGICAL_PLANNER_TIME_NANOS,
                () -> logicalPlanner.plan(analysis));

        Optimizer optimizer = new Optimizer(
                session,
                metadata,
                optimizers.getPlanningTimeOptimizers(),
                planChecker,
                sqlParser,
                variableAllocator,
                idAllocator,
                warningCollector,
                statsCalculator,
                costCalculator,
                false);

        Plan plan = session.getRuntimeStats().profileNanos(
                OPTIMIZER_TIME_NANOS,
                () -> optimizer.validateAndOptimizePlan(planNode, OPTIMIZED_AND_VALIDATED));

        List<Input> inputs = new InputExtractor(metadata, session).extractInputs(plan.getRoot());
        Optional<Output> output = new OutputExtractor().extractOutput(plan.getRoot());
        Optional<QueryType> queryType = getQueryType(preparedQuery.getStatement().getClass());
        List<String> columnNames = ((OutputNode) plan.getRoot()).getColumnNames();
        PrestoSparkPhysicalResourceCalculator prestoSparkPhysicalResourceCalculator = new PrestoSparkPhysicalResourceCalculator(
                getHashPartitionCount(session),
                sparkContext.getConf().getInt(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG, 0));
        PhysicalResourceSettings physicalResourceSettings = prestoSparkPhysicalResourceCalculator
                .calculate(plan.getRoot(), new PrestoSparkSourceStatsCollector(metadata, session), session);
        Map<FunctionKind, Set<String>> functionsInvoked = Collections.emptyMap();
        if (isLogInvokedFunctionNamesEnabled(session)) {
            functionsInvoked = analysis.getInvokedFunctions();
        }
        Set<String> invokedScalarFunctions = functionsInvoked.getOrDefault(SCALAR, Collections.emptySet());
        Set<String> invokedAggregateFunctions = functionsInvoked.getOrDefault(AGGREGATE, Collections.emptySet());
        Set<String> invokedWindowFunctions = functionsInvoked.getOrDefault(WINDOW, Collections.emptySet());

        return new PlanAndMore(
                plan,
                Optional.ofNullable(analysis.getUpdateType()),
                columnNames,
                ImmutableSet.copyOf(inputs),
                output,
                queryType,
                physicalResourceSettings,
                getCanonicalInfo(session, plan.getRoot(), planCanonicalInfoProvider),
                invokedScalarFunctions,
                invokedAggregateFunctions,
                invokedWindowFunctions);
    }

    public static class PlanAndMore
    {
        private final Plan plan;
        private final Optional<String> updateType;
        private final List<String> fieldNames;
        private final Set<Input> inputs;
        private final Optional<Output> output;
        private final Optional<QueryType> queryType;
        private final PhysicalResourceSettings physicalResourceSettings;
        private final List<CanonicalPlanWithInfo> planCanonicalInfo;
        private final Set<String> invokedScalarFunctions;
        private final Set<String> invokedAggregateFunctions;
        private final Set<String> invokedWindowFunctions;

        public PlanAndMore(
                Plan plan,
                Optional<String> updateType,
                List<String> fieldNames,
                Set<Input> inputs,
                Optional<Output> output,
                Optional<QueryType> queryType,
                PhysicalResourceSettings physicalResourceSettings,
                List<CanonicalPlanWithInfo> planCanonicalInfo,
                Set<String> invokedScalarFunctions,
                Set<String> invokedAggregateFunctions,
                Set<String> invokedWindowFunctions)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.updateType = requireNonNull(updateType, "updateType is null");
            this.fieldNames = ImmutableList.copyOf(requireNonNull(fieldNames, "fieldNames is null"));
            this.inputs = ImmutableSet.copyOf(requireNonNull(inputs, "inputs is null"));
            this.output = requireNonNull(output, "output is null");
            this.queryType = requireNonNull(queryType, "queryType is null");
            this.physicalResourceSettings = requireNonNull(physicalResourceSettings, "physicalResourceSetting is null.");
            this.planCanonicalInfo = requireNonNull(planCanonicalInfo, "planCanonicalInfo is null");
            this.invokedScalarFunctions = requireNonNull(invokedScalarFunctions, "invokedScalarFunctions is null");
            this.invokedAggregateFunctions = requireNonNull(invokedAggregateFunctions, "invokedAggregateFunctions is null");
            this.invokedWindowFunctions = requireNonNull(invokedWindowFunctions, "invokedWindowFunctions is null");
        }

        public Plan getPlan()
        {
            return plan;
        }

        public Optional<String> getUpdateType()
        {
            return updateType;
        }

        public List<String> getFieldNames()
        {
            return fieldNames;
        }

        public Set<Input> getInputs()
        {
            return inputs;
        }

        public Optional<Output> getOutput()
        {
            return output;
        }

        public Optional<QueryType> getQueryType()
        {
            return queryType;
        }

        public PhysicalResourceSettings getPhysicalResourceSettings()
        {
            return physicalResourceSettings;
        }

        public List<CanonicalPlanWithInfo> getPlanCanonicalInfo()
        {
            return planCanonicalInfo;
        }

        public Set<String> getInvokedScalarFunctions()
        {
            return invokedScalarFunctions;
        }

        public Set<String> getInvokedAggregateFunctions()
        {
            return invokedAggregateFunctions;
        }

        public Set<String> getInvokedWindowFunctions()
        {
            return invokedWindowFunctions;
        }
    }
}
