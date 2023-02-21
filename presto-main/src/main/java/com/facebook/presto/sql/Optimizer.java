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
package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.sanity.PlanChecker;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isPrintStatsForNonJoinQuery;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Optimizer
{
    public enum PlanStage
    {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    private final List<PlanOptimizer> planOptimizers;
    private final PlanChecker planChecker;
    private final Session session;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final PlanVariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final WarningCollector warningCollector;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final boolean explain;

    public Optimizer(
            Session session,
            Metadata metadata,
            List<PlanOptimizer> planOptimizers,
            PlanChecker planChecker,
            SqlParser sqlParser,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            boolean explain)
    {
        this.session = requireNonNull(session, "session is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.explain = explain;
    }

    public Plan validateAndOptimizePlan(PlanNode root, PlanStage stage)
    {
        planChecker.validateIntermediatePlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);

        boolean enableVerboseRuntimeStats = SystemSessionProperties.isVerboseRuntimeStatsEnabled(session);
        if (stage.ordinal() >= OPTIMIZED.ordinal()) {
            for (PlanOptimizer optimizer : planOptimizers) {
                long start = System.nanoTime();
                root = optimizer.optimize(root, session, variableAllocator.getTypes(), variableAllocator, idAllocator, warningCollector);
                requireNonNull(root, format("%s returned a null plan", optimizer.getClass().getName()));
                if (enableVerboseRuntimeStats) {
                    session.getRuntimeStats().addMetricValue(String.format("optimizer%sTimeNanos", optimizer.getClass().getSimpleName()), NANO, System.nanoTime() - start);
                }
            }
        }

        if (stage.ordinal() >= OPTIMIZED_AND_VALIDATED.ordinal()) {
            // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
            planChecker.validateFinalPlan(root, session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);
        }

        TypeProvider types = variableAllocator.getTypes();
        return new Plan(root, types, computeStats(root, types));
    }

    private StatsAndCosts computeStats(PlanNode root, TypeProvider types)
    {
        if (explain || isPrintStatsForNonJoinQuery(session) ||
                PlanNodeSearcher.searchFrom(root).where(node ->
                        (node instanceof JoinNode) || (node instanceof SemiJoinNode)).matches()) {
            StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
            CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.empty(), session);
            return StatsAndCosts.create(root, statsProvider, costProvider);
        }
        return StatsAndCosts.empty();
    }
}
