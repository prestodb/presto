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
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType.Type;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExplainer
{
    private final List<PlanOptimizer> planOptimizers;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final boolean experimentalSyntaxEnabled;
    private final Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask;

    @Inject
    public QueryExplainer(
            List<PlanOptimizer> planOptimizers,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask,
            FeaturesConfig featuresConfig)
    {
        this(planOptimizers,
                metadata,
                sqlParser,
                dataDefinitionTask,
                featuresConfig.isExperimentalSyntaxEnabled());
    }

    public QueryExplainer(
            List<PlanOptimizer> planOptimizers,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Class<? extends Statement>, DataDefinitionTask<?>> dataDefinitionTask,
            boolean experimentalSyntaxEnabled)
    {
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
        this.dataDefinitionTask = ImmutableMap.copyOf(checkNotNull(dataDefinitionTask, "dataDefinitionTask is null"));
    }

    public String getPlan(Session session, Statement statement, Type planType)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            return explainTask(statement, task);
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement);
                return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, session);
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement);
                return PlanPrinter.textDistributedPlan(subPlan, metadata, session);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    private static <T extends Statement> String explainTask(Statement statement, DataDefinitionTask<T> task)
    {
        return task.explain((T) statement);
    }

    public String getGraphvizPlan(Session session, Statement statement, Type planType)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            // todo format as graphviz
            return explainTask(statement, task);
        }

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement);
                return PlanPrinter.graphvizLogicalPlan(plan.getRoot(), plan.getTypes());
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement);
                return PlanPrinter.graphvizDistributedPlan(subPlan);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public String getJsonPlan(Session session, Statement statement)
    {
        DataDefinitionTask<?> task = dataDefinitionTask.get(statement.getClass());
        if (task != null) {
            return "{}";
        }

        Plan plan = getLogicalPlan(session, statement);
        return PlanPrinter.getJsonPlanSource(plan.getRoot(), metadata, null);
    }

    private Plan getLogicalPlan(Session session, Statement statement)
    {
        // analyze statement
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(this), experimentalSyntaxEnabled);

        Analysis analysis = analyzer.analyze(statement);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan statement
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata);
        return logicalPlanner.plan(analysis);
    }

    private SubPlan getDistributedPlan(Session session, Statement statement)
    {
        // analyze statement
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, Optional.of(this), experimentalSyntaxEnabled);

        Analysis analysis = analyzer.analyze(statement);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan statement
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata);
        Plan plan = logicalPlanner.plan(analysis);

        return new PlanFragmenter().createSubPlans(plan);
    }
}
