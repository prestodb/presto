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
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
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

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    public SubPlan createSubPlans(Plan plan)
    {
        Fragmenter fragmenter = new Fragmenter(plan.getSymbolAllocator().getTypes());

        FragmentProperties properties = new FragmentProperties();
        PlanNode root = PlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan result = fragmenter.buildFragment(root, properties);
        result.sanityCheck();

        return result;
    }

    private static class Fragmenter
            extends PlanRewriter<FragmentProperties>
    {
        private final Map<Symbol, Type> types;
        private int nextFragmentId;

        public Fragmenter(Map<Symbol, Type> types)
        {
            this.types = types;
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(String.valueOf(nextFragmentId++));
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties)
        {
            Set<Symbol> dependencies = SymbolExtractor.extract(root);

            PlanFragment fragment = new PlanFragment(
                    nextFragmentId(),
                    root,
                    Maps.filterKeys(types, in(dependencies)),
                    properties.getOutputLayout(),
                    properties.getDistribution(),
                    properties.getDistributeBy(),
                    properties.getOutputPartitioning(),
                    properties.getPartitionBy(),
                    properties.getHash());

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            context.get()
                    .setSingleNodeDistribution() // TODO: add support for distributed output
                    .setOutputLayout(node.getOutputSymbols())
                    .setUnpartitionedOutput();

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableCommit(TableCommitNode node, RewriteContext<FragmentProperties> context)
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
            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();

                for (int i = 0; i < exchange.getSources().size(); i++) {
                    FragmentProperties childProperties = new FragmentProperties();
                    childProperties.setUnpartitionedOutput();
                    childProperties.setOutputLayout(exchange.getInputs().get(i));

                    builder.add(buildSubPlan(exchange.getSources().get(i), childProperties, context));
                }
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setFixedDistribution();

                FragmentProperties childProperties = new FragmentProperties()
                        .setHashPartitionedOutput(exchange.getPartitionKeys(), exchange.getHashSymbol())
                        .setOutputLayout(Iterables.getOnlyElement(exchange.getInputs()));

                builder.add(buildSubPlan(Iterables.getOnlyElement(exchange.getSources()), childProperties, context));
            }
            else if (exchange.getType() == ExchangeNode.Type.REPLICATE) {
                FragmentProperties childProperties = new FragmentProperties();
                childProperties.setUnpartitionedOutput();
                childProperties.setOutputLayout(Iterables.getOnlyElement(exchange.getInputs()));

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
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties);
        }
    }

    private static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private Optional<List<Symbol>> outputLayout = Optional.empty();
        private Optional<OutputPartitioning> outputPartitioning = Optional.empty();

        private List<Symbol> partitionBy = ImmutableList.of();
        private Optional<Symbol> hash = Optional.empty();

        private Optional<PlanDistribution> distribution = Optional.empty();
        private PlanNodeId distributeBy;

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (distribution.isPresent()) {
                PlanDistribution value = distribution.get();
                checkState(value == PlanDistribution.SINGLE || value == PlanDistribution.COORDINATOR_ONLY,
                        "Cannot overwrite distribution with %s (currently set to %s)", PlanDistribution.SINGLE, value);
            }
            else {
                distribution = Optional.of(PlanDistribution.SINGLE);
            }

            return this;
        }

        public FragmentProperties setFixedDistribution()
        {
            distribution.ifPresent(current -> checkState(current == PlanDistribution.FIXED,
                    "Cannot set distribution to %s. Already set to %s",
                    PlanDistribution.FIXED,
                    current));

            distribution = Optional.of(PlanDistribution.FIXED);

            return this;
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            // only SINGLE can be upgraded to COORDINATOR_ONLY
            distribution.ifPresent(current -> checkState(distribution.get() == PlanDistribution.SINGLE,
                    "Cannot overwrite distribution with %s (currently set to %s)",
                    PlanDistribution.COORDINATOR_ONLY,
                    distribution.get()));

            distribution = Optional.of(PlanDistribution.COORDINATOR_ONLY);

            return this;
        }

        public FragmentProperties setSourceDistribution(PlanNodeId source)
        {
            if (distribution.isPresent()) {
                // If already SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
                checkState(distribution.get() == PlanDistribution.SINGLE || distribution.get() == PlanDistribution.COORDINATOR_ONLY,
                        "Cannot overwrite distribution with %s (currently set to %s)",
                        PlanDistribution.SOURCE,
                        distribution.get());
            }
            else {
                distribution = Optional.of(PlanDistribution.SOURCE);
                this.distributeBy = source;
            }

            return this;
        }

        public FragmentProperties setUnpartitionedOutput()
        {
            outputPartitioning.ifPresent(current -> {
                throw new IllegalStateException(String.format("Output overwrite partitioning with %s (currently set to %s)", OutputPartitioning.NONE, current));
            });

            outputPartitioning = Optional.of(OutputPartitioning.NONE);

            return this;
        }

        public FragmentProperties setOutputLayout(List<Symbol> layout)
        {
            outputLayout.ifPresent(current -> {
                throw new IllegalStateException(String.format("Cannot overwrite output layout with %s (currently set to %s)", layout, current));
            });

            outputLayout = Optional.of(layout);

            return this;
        }

        public FragmentProperties setHashPartitionedOutput(List<Symbol> partitionKeys, Optional<Symbol> hash)
        {
            outputPartitioning.ifPresent(current -> {
                throw new IllegalStateException(String.format("Cannot overwrite output partitioning with %s (currently set to %s)", OutputPartitioning.HASH, current));
            });

            this.outputPartitioning = Optional.of(OutputPartitioning.HASH);
            this.partitionBy = ImmutableList.copyOf(partitionKeys);
            this.hash = hash;

            return this;
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public List<Symbol> getOutputLayout()
        {
            return outputLayout.get();
        }

        public OutputPartitioning getOutputPartitioning()
        {
            return outputPartitioning.get();
        }

        public PlanDistribution getDistribution()
        {
            return distribution.get();
        }

        public List<Symbol> getPartitionBy()
        {
            return partitionBy;
        }

        public Optional<Symbol> getHash()
        {
            return hash;
        }

        public PlanNodeId getDistributeBy()
        {
            return distributeBy;
        }
    }
}
