package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.Sitevars;
import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneRedundantProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.SetFlatteningOptimizer;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.TableAliasSelector;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Provider;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class PlanOptimizersFactory
        implements Provider<List<PlanOptimizer>>
{
    private final Metadata metadata;

    private List<PlanOptimizer> optimizers;

    @Inject
    public PlanOptimizersFactory(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new SimplifyExpressions(metadata),
                new PruneUnreferencedOutputs(),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new MergeProjections(),
                new SetFlatteningOptimizer(),
                new LimitPushDown()); // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    @Inject(optional = true)
    public synchronized void injectAdditionalDependencies(AliasDao aliasDao,
            NodeManager nodeManager,
            ShardManager shardManager,
            Sitevars sitevars)
    {
        checkNotNull(aliasDao, "aliasDao is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(shardManager, "shardManager is null");
        checkNotNull(sitevars, "sitevars is null");

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        builder.addAll(optimizers);
        builder.add(new TableAliasSelector(metadata, aliasDao, nodeManager, shardManager, sitevars));

        this.optimizers = builder.build();
    }

    public synchronized List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
