package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.optimizations.CoalesceLimits;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneRedundantProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.TableAliasSelector;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Singleton;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class PlanOptimizersFactory
{
    private final Metadata metadata;

    private AliasDao aliasDao;
    private NodeManager nodeManager;
    private ShardManager shardManager;

    @Inject
    public PlanOptimizersFactory(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Inject(optional = true)
    public void injectAdditionalDependencies(AliasDao aliasDao,
            NodeManager nodeManager,
            ShardManager shardManager)
    {
        this.aliasDao = aliasDao;
        this.nodeManager = nodeManager;
        this.shardManager = shardManager;
    }

    public List<PlanOptimizer> createOptimizations(Session session)
    {
        checkNotNull(session, "session is null");

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new SimplifyExpressions(metadata, session),
                new PruneUnreferencedOutputs(),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new CoalesceLimits());

        if (aliasDao != null && nodeManager != null && shardManager != null) {
            builder.add(new TableAliasSelector(metadata, aliasDao, nodeManager, shardManager));
        }

        return builder.build();
    }
}