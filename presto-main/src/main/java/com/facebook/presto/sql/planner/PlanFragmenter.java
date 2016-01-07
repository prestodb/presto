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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    public SubPlan createSubPlans(Plan plan)
    {
        Fragmenter fragmenter = new Fragmenter(plan.getSymbolAllocator().getTypes());

        FragmentProperties properties = new FragmentProperties(new PartitionFunctionBinding(SINGLE_DISTRIBUTION, plan.getRoot().getOutputSymbols(), ImmutableList.of()))
                .setSingleNodeDistribution();
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan result = fragmenter.buildRootFragment(root, properties);
        result.sanityCheck();

        return result;
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Map<Symbol, Type> types;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, new PlanFragmentId(String.valueOf(ROOT_FRAGMENT_ID)));
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(String.valueOf(nextFragmentId++));
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            Set<Symbol> dependencies = SymbolExtractor.extract(root);

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    Maps.filterKeys(types, in(dependencies)),
                    properties.getPartitioningHandle(),
                    properties.getDistributeBy(),
                    properties.getPartitionFunction());

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSingleNodeDistribution(); // TODO: add support for distributed output

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMetadataDelete(MetadataDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSourceDistribution(node.getId());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSingleNodeDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            PartitionFunctionBinding partitionFunction = exchange.getPartitionFunction();

            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();

                for (int i = 0; i < exchange.getSources().size(); i++) {
                    FragmentProperties childProperties = new FragmentProperties(partitionFunction.translateOutputLayout(exchange.getInputs().get(i)));
                    builder.add(buildSubPlan(exchange.getSources().get(i), childProperties, context));
                }
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(partitionFunction.getPartitioningHandle());

                FragmentProperties childProperties = new FragmentProperties(partitionFunction.translateOutputLayout(Iterables.getOnlyElement(exchange.getInputs())));
                builder.add(buildSubPlan(Iterables.getOnlyElement(exchange.getSources()), childProperties, context));
            }
            else if (exchange.getType() == ExchangeNode.Type.REPLICATE) {
                FragmentProperties childProperties = new FragmentProperties(partitionFunction.translateOutputLayout(Iterables.getOnlyElement(exchange.getInputs())));
                builder.add(buildSubPlan(Iterables.getOnlyElement(exchange.getSources()), childProperties, context));
            }

            List<SubPlan> children = builder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols());
        }

        private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
        {
            PlanFragmentId planFragmentId = nextFragmentId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId);
        }
    }

    private static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private final PartitionFunctionBinding partitionFunction;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private PlanNodeId distributeBy;

        public FragmentProperties(PartitionFunctionBinding partitionFunction)
        {
            this.partitionFunction = partitionFunction;
        }

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(!partitioningHandle.isPresent(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(PartitioningHandle distribution)
        {
            if (partitioningHandle.isPresent() && !partitioningHandle.get().equals(distribution) && !partitioningHandle.get().equals(SOURCE_DISTRIBUTION)) {
                checkState(partitioningHandle.get().isSingleNode(),
                        "Cannot set distribution to %s. Already set to %s",
                        distribution,
                        partitioningHandle);
                return this;
            }
            partitioningHandle = Optional.of(distribution);

            return this;
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(!partitioningHandle.isPresent() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setSourceDistribution(PlanNodeId source)
        {
            if (partitioningHandle.isPresent()) {
                PartitioningHandle partitioningHandle = this.partitioningHandle.get();
                if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
                    checkState(distributeBy == null || distributeBy == source, "Cannot overwrite partitioned source");
                }
                else {
                    // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
                    checkState(
                            partitioningHandle.equals(SINGLE_DISTRIBUTION) || partitioningHandle.equals(COORDINATOR_DISTRIBUTION),
                            "Cannot overwrite distribution with %s (currently set to %s)",
                            SOURCE_DISTRIBUTION,
                            partitioningHandle);
                    return this;
                }
            }
            distributeBy = requireNonNull(source, "source is null");
            partitioningHandle = Optional.of(SOURCE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public PartitionFunctionBinding getPartitionFunction()
        {
            return partitionFunction;
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle.get();
        }

        public PlanNodeId getDistributeBy()
        {
            return distributeBy;
        }
    }
}
