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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ApplyConnectorOptimization
        implements PlanOptimizer
{
    static final Set<Class<? extends PlanNode>> CONNECTOR_ACCESSIBLE_PLAN_NODES = ImmutableSet.of(
            DistinctLimitNode.class,
            FilterNode.class,
            TableScanNode.class,
            LimitNode.class,
            TopNNode.class,
            ValuesNode.class,
            ProjectNode.class,
            AggregationNode.class,
            MarkDistinctNode.class,
            UnionNode.class,
            IntersectNode.class,
            ExceptNode.class);

    // for a leaf node that does not belong to any connector (e.g., ValuesNode)
    private static final ConnectorId EMPTY_CONNECTOR_ID = new ConnectorId("$internal$" + ApplyConnectorOptimization.class + "_CONNECTOR");

    private final Supplier<Map<ConnectorId, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier;

    public ApplyConnectorOptimization(Supplier<Map<ConnectorId, Set<ConnectorPlanOptimizer>>> connectorOptimizersSupplier)
    {
        this.connectorOptimizersSupplier = requireNonNull(connectorOptimizersSupplier, "connectorOptimizersSupplier is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        Map<ConnectorId, Set<ConnectorPlanOptimizer>> connectorOptimizers = connectorOptimizersSupplier.get();
        if (connectorOptimizers.isEmpty()) {
            return plan;
        }

        // retrieve all the connectors
        ImmutableSet.Builder<ConnectorId> connectorIds = ImmutableSet.builder();
        getAllConnectorIds(plan, connectorIds);

        // for each connector, retrieve the set of subplans to optimize
        // TODO: what if a new connector is added by an existing one
        // There are cases (e.g., query federation) where a connector C1 needs to
        // create a UNION_ALL to federate data sources from both C1 and C2 (regardless of the classloader issue).
        // For such case, it is dangerous to re-calculate the "max closure" given the fixpoint property will be broken.
        // In order to preserve the fixpoint, we will "pretend" the newly added C2 table scan is part of C1's job to maintain.
        for (ConnectorId connectorId : connectorIds.build()) {
            Set<ConnectorPlanOptimizer> optimizers = connectorOptimizers.get(connectorId);
            if (optimizers == null) {
                continue;
            }

            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextMapBuilder = ImmutableMap.builder();
            buildConnectorPlanNodeContext(plan, null, contextMapBuilder);
            Map<PlanNode, ConnectorPlanNodeContext> contextMap = contextMapBuilder.build();

            // keep track of changed nodes; the keys are original nodes and the values are the new nodes
            Map<PlanNode, PlanNode> updates = new HashMap<>();

            // process connector optimizers
            for (PlanNode node : contextMap.keySet()) {
                // For a subtree with root `node` to be a max closure, the following conditions must hold:
                //    * The subtree with root `node` is a closure.
                //    * `node` has no parent, or the subtree with root as `node`'s parent is not a closure.
                ConnectorPlanNodeContext context = contextMap.get(node);
                if (!context.isClosure(connectorId) ||
                        !context.getParent().isPresent() ||
                        contextMap.get(context.getParent().get()).isClosure(connectorId)) {
                    continue;
                }

                PlanNode newNode = node;

                // the returned node is still a max closure (only if there is no new connector added, which does happen but ignored here)
                for (ConnectorPlanOptimizer optimizer : optimizers) {
                    newNode = optimizer.optimize(newNode, session.toConnectorSession(connectorId), variableAllocator, idAllocator);
                }

                if (node != newNode) {
                    // the optimizer has allocated a new PlanNode
                    checkState(
                            containsAll(ImmutableSet.copyOf(newNode.getOutputVariables()), node.getOutputVariables()),
                            "the connector optimizer from %s returns a node that does not cover all output before optimization",
                            connectorId);
                    updates.put(node, newNode);
                }
            }
            // up to this point, we have a set of updated nodes; need to recursively update their parents

            // alter the plan with a bottom-up approach (but does not have to be strict bottom-up to guarantee the correctness of the algorithm)
            // use "original nodes" to keep track of the plan structure and "updates" to keep track of the new nodes
            Queue<PlanNode> originalNodes = new LinkedList<>(updates.keySet());
            while (!originalNodes.isEmpty()) {
                PlanNode originalNode = originalNodes.poll();

                if (!contextMap.get(originalNode).getParent().isPresent()) {
                    // originalNode must be the root; update the plan
                    plan = updates.get(originalNode);
                    continue;
                }

                PlanNode originalParent = contextMap.get(originalNode).getParent().get();

                // need to create a new parent given the child has changed; the new parent needs to point to the new child.
                // if a node has been updated, it will occur in `updates`; otherwise, just use the original node
                ImmutableList.Builder<PlanNode> newChildren = ImmutableList.builder();
                originalParent.getSources().forEach(child -> newChildren.add(updates.getOrDefault(child, child)));
                PlanNode newParent = originalParent.replaceChildren(newChildren.build());

                // mark the new parent as updated
                updates.put(originalParent, newParent);

                // enqueue the parent node in order to recursively update its ancestors
                originalNodes.add(originalParent);
            }
        }

        return plan;
    }

    private static void getAllConnectorIds(PlanNode node, ImmutableSet.Builder<ConnectorId> builder)
    {
        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                builder.add(((TableScanNode) node).getTable().getConnectorId());
            }
            else {
                builder.add(EMPTY_CONNECTOR_ID);
            }
            return;
        }

        for (PlanNode child : node.getSources()) {
            getAllConnectorIds(child, builder);
        }
    }

    private static ConnectorPlanNodeContext buildConnectorPlanNodeContext(
            PlanNode node,
            PlanNode parent,
            ImmutableMap.Builder<PlanNode, ConnectorPlanNodeContext> contextBuilder)
    {
        Set<ConnectorId> connectorIds;
        Set<Class<? extends PlanNode>> planNodeTypes;

        if (node.getSources().isEmpty()) {
            if (node instanceof TableScanNode) {
                connectorIds = ImmutableSet.of(((TableScanNode) node).getTable().getConnectorId());
                planNodeTypes = ImmutableSet.of(TableScanNode.class);
            }
            else {
                connectorIds = ImmutableSet.of(EMPTY_CONNECTOR_ID);
                planNodeTypes = ImmutableSet.of(node.getClass());
            }
        }
        else {
            connectorIds = new HashSet<>();
            planNodeTypes = new HashSet<>();

            for (PlanNode child : node.getSources()) {
                ConnectorPlanNodeContext childContext = buildConnectorPlanNodeContext(child, node, contextBuilder);
                connectorIds.addAll(childContext.getReachableConnectors());
                planNodeTypes.addAll(childContext.getReachablePlanNodeTypes());
            }
            planNodeTypes.add(node.getClass());
        }

        ConnectorPlanNodeContext connectorPlanNodeContext = new ConnectorPlanNodeContext(
                parent,
                connectorIds,
                planNodeTypes);

        contextBuilder.put(node, connectorPlanNodeContext);
        return connectorPlanNodeContext;
    }

    /**
     * Extra information needed for a plan node
     */
    private static final class ConnectorPlanNodeContext
    {
        private final PlanNode parent;
        private final Set<ConnectorId> reachableConnectors;
        private final Set<Class<? extends PlanNode>> reachablePlanNodeTypes;

        ConnectorPlanNodeContext(PlanNode parent, Set<ConnectorId> reachableConnectors, Set<Class<? extends PlanNode>> reachablePlanNodeTypes)
        {
            this.parent = parent;
            this.reachableConnectors = requireNonNull(reachableConnectors, "reachableConnectors is null");
            this.reachablePlanNodeTypes = requireNonNull(reachablePlanNodeTypes, "reachablePlanNodeTypes is null");
            checkArgument(!reachableConnectors.isEmpty(), "encountered a PlanNode that reaches no connector");
            checkArgument(!reachablePlanNodeTypes.isEmpty(), "encountered a PlanNode that reaches no plan node");
        }

        Optional<PlanNode> getParent()
        {
            return Optional.ofNullable(parent);
        }

        public Set<ConnectorId> getReachableConnectors()
        {
            return reachableConnectors;
        }

        public Set<Class<? extends PlanNode>> getReachablePlanNodeTypes()
        {
            return reachablePlanNodeTypes;
        }

        boolean isClosure(ConnectorId connectorId)
        {
            // check if all children can reach the only connector
            if (reachableConnectors.size() != 1 || !reachableConnectors.contains(connectorId)) {
                return false;
            }

            // check if all children are accessible by connectors
            return containsAll(CONNECTOR_ACCESSIBLE_PLAN_NODES, reachablePlanNodeTypes);
        }
    }

    private static <T> boolean containsAll(Set<T> container, Collection<T> test)
    {
        for (T element : test) {
            if (!container.contains(element)) {
                return false;
            }
        }
        return true;
    }
}
