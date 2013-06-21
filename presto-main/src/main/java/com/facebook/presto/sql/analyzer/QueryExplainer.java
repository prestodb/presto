package com.facebook.presto.sql.analyzer;

import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
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

    public String getPlan(Query query)
    {
        // analyze query
        Analyzer analyzer = new Analyzer(session, metadata, Optional.<QueryExplainer>absent());

        Analysis analysis = analyzer.analyze(query);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, periodicImportManager, storageManager);
        Plan plan = logicalPlanner.plan(analysis);

        return PlanPrinter.printPlan(plan.getRoot(), plan.getTypes());
    }

}
