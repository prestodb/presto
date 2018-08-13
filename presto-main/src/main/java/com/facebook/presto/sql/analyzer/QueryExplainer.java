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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.tree.ExplainType.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.textIOPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryExplainer
{
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final InternalNodeManager nodeManager;
    private final NodeSchedulerConfig nodeSchedulerConfig;
    private final Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask;

    @Inject
    public QueryExplainer(
            PlanOptimizers planOptimizers,
            PlanFragmenter planFragmenter,
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            AccessControl accessControl,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            InternalNodeManager nodeManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask)
    {
        this(
                planOptimizers.get(),
                planFragmenter,
                metadata,
                nodePartitioningManager,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator,
                nodeManager,
                nodeSchedulerConfig,
                dataDefinitionTask);
    }

    public QueryExplainer(
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            AccessControl accessControl,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            InternalNodeManager nodeManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask)
    {
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeSchedulerConfig = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null");
        this.dataDefinitionTask = ImmutableMap.copyOf(requireNonNull(dataDefinitionTask, "dataDefinitionTask is null"));
    }

    public Analysis analyze(Session session, Statement statement, List<Expression> parameters)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(this), parameters);
        return analyzer.analyze(statement);
    }

    public String getPlan(Session session, Statement statement, Type planType, List<Expression> parameters)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            return explainTask(statement, task, parameters);
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement, parameters);
                return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionRegistry(), statsCalculator, costCalculator, session, 0, false);
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters);
                return PlanPrinter.textDistributedPlan(subPlan, metadata.getFunctionRegistry(), statsCalculator, costCalculator, nodeManager, nodeSchedulerConfig, session, false);
            case IO:
                return IOPlanPrinter.textIOPlan(getLogicalPlan(session, statement, parameters).getRoot(), metadata, session);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    private static <T extends Statement> String explainTask(Statement statement, DataDefinitionTask<T> task, List<Expression> parameters)
    {
        return task.explain((T) statement, parameters);
    }

    public String getGraphvizPlan(Session session, Statement statement, Type planType, List<Expression> parameters)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            // todo format as graphviz
            return explainTask(statement, task, parameters);
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement, parameters);
                return PlanPrinter.graphvizLogicalPlan(plan.getRoot(), plan.getTypes());
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters);
                return PlanPrinter.graphvizDistributedPlan(subPlan);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public String getJsonPlan(Session session, Statement statement, Type planType, List<Expression> parameters)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            // todo format as json
            return explainTask(statement, task, parameters);
        }

        switch (planType) {
            case IO:
                Plan plan = getLogicalPlan(session, statement, parameters);
                return textIOPlan(plan.getRoot(), metadata, session);
            default:
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported explain plan type %s for JSON format", planType));
        }
    }

    public Plan getLogicalPlan(Session session, Statement statement, List<Expression> parameters)
    {
        // analyze statement
        Analysis analysis = analyze(session, statement, parameters);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan statement
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, sqlParser);
        return logicalPlanner.plan(analysis);
    }

    private SubPlan getDistributedPlan(Session session, Statement statement, List<Expression> parameters)
    {
        Plan plan = getLogicalPlan(session, statement, parameters);
        return planFragmenter.createSubPlans(session, metadata, nodePartitioningManager, plan, false);
    }
}
