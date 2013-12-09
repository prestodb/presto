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

import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExplainer
{
    public final Session session;
    public final List<PlanOptimizer> planOptimizers;
    public final Metadata metadata;
    public final PeriodicImportManager periodicImportManager;
    public final StorageManager storageManager;

    public QueryExplainer(Session session,
            List<PlanOptimizer> planOptimizers,
            Metadata metadata,
            PeriodicImportManager periodicImportManager,
            StorageManager storageManager)
    {
        this.session = checkNotNull(session, "session is null");
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    public String getPlan(Query query, ExplainType.Type planType)
    {
        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(query);
                return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes());
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(query);
                return PlanPrinter.textDistributedPlan(subPlan);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public String getGraphvizPlan(Query query, ExplainType.Type planType)
    {
        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(query);
                return PlanPrinter.graphvizLogicalPlan(plan.getRoot(), plan.getTypes());
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(query);
                return PlanPrinter.graphvizDistributedPlan(subPlan);
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    private Plan getLogicalPlan(Query query)
    {
        // analyze query
        Analyzer analyzer = new Analyzer(session, metadata, Optional.of(this));

        Analysis analysis = analyzer.analyze(query);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, periodicImportManager, storageManager);
        return logicalPlanner.plan(analysis);
    }

    private SubPlan getDistributedPlan(Query query)
    {
        // analyze query
        Analyzer analyzer = new Analyzer(session, metadata, Optional.of(this));

        Analysis analysis = analyzer.analyze(query);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, periodicImportManager, storageManager);
        Plan plan = logicalPlanner.plan(analysis);

        return new DistributedLogicalPlanner(metadata, idAllocator).createSubPlans(plan, false);
    }
}
