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

import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.optimizations.ImplementSampleAsFilter;
import com.facebook.presto.sql.planner.optimizations.LimitPushDown;
import com.facebook.presto.sql.planner.optimizations.MergeProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
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
    public PlanOptimizersFactory(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        builder.add(new ImplementSampleAsFilter(),
                new SimplifyExpressions(metadata),
                new PruneUnreferencedOutputs(),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new SetFlatteningOptimizer(),
                new LimitPushDown(), // Run the LimitPushDown after flattening set operators to make it easier to do the set flattening
                new PredicatePushDown(metadata, splitManager),
                new PredicatePushDown(metadata, splitManager), // Run predicate push down one more time in case we can leverage new information from generated partitions
                new MergeProjections(),
                new SimplifyExpressions(metadata), // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new UnaliasSymbolReferences(), // Run again because predicate pushdown might add more projections
                new PruneUnreferencedOutputs(), // Prune outputs again in case predicate pushdown move predicates all the way into the table scan
                new PruneRedundantProjections()); // Run again because predicate pushdown might add more projections
        // TODO: figure out how to improve the set flattening optimizer so that it can run at any point

        this.optimizers = builder.build();
    }

    @Inject(optional = true)
    public synchronized void injectAdditionalDependencies(AliasDao aliasDao, NodeManager nodeManager, ShardManager shardManager)
    {
        checkNotNull(aliasDao, "aliasDao is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(shardManager, "shardManager is null");

        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();
        builder.addAll(optimizers);
        builder.add(new TableAliasSelector(metadata, aliasDao, nodeManager, shardManager));

        this.optimizers = builder.build();
    }

    @Override
    public synchronized List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
