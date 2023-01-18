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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StarJoinNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.KosarajuStrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

@NotThreadSafe
public class PhasedExecutionSchedule
        implements ExecutionSchedule
{
    private final List<Set<StageExecutionAndScheduler>> schedulePhases;
    private final Set<StageExecutionAndScheduler> activeSources = new HashSet<>();

    public PhasedExecutionSchedule(Collection<StageExecutionAndScheduler> stages)
    {
        List<Set<PlanFragmentId>> phases = extractPhases(stages.stream()
                .map(StageExecutionAndScheduler::getStageExecution)
                .map(SqlStageExecution::getFragment)
                .collect(toImmutableList()));

        Map<PlanFragmentId, StageExecutionAndScheduler> stagesByFragmentId = stages.stream().collect(toImmutableMap(stage -> stage.getStageExecution().getFragment().getId(), identity()));

        // create a mutable list of mutable sets of stages, so we can remove completed stages
        schedulePhases = new ArrayList<>();
        for (Set<PlanFragmentId> phase : phases) {
            schedulePhases.add(phase.stream()
                    .map(stagesByFragmentId::get)
                    .collect(Collectors.toCollection(HashSet::new)));
        }
    }

    @Override
    public Set<StageExecutionAndScheduler> getStagesToSchedule()
    {
        removeCompletedStages();
        addPhasesIfNecessary();
        if (isFinished()) {
            return ImmutableSet.of();
        }
        return activeSources;
    }

    private void removeCompletedStages()
    {
        for (Iterator<StageExecutionAndScheduler> stageIterator = activeSources.iterator(); stageIterator.hasNext(); ) {
            StageExecutionState state = stageIterator.next().getStageExecution().getState();
            if (state == SCHEDULED || state == RUNNING || state.isDone()) {
                stageIterator.remove();
            }
        }
    }

    private void addPhasesIfNecessary()
    {
        // we want at least one source distributed phase in the active sources
        if (hasSourceDistributedStage(activeSources)) {
            return;
        }

        while (!schedulePhases.isEmpty()) {
            Set<StageExecutionAndScheduler> phase = schedulePhases.remove(0);
            activeSources.addAll(phase);
            if (hasSourceDistributedStage(phase)) {
                return;
            }
        }
    }

    private static boolean hasSourceDistributedStage(Set<StageExecutionAndScheduler> phase)
    {
        return phase.stream().anyMatch(stage -> !stage.getStageExecution().getFragment().getTableScanSchedulingOrder().isEmpty());
    }

    @Override
    public boolean isFinished()
    {
        return activeSources.isEmpty() && schedulePhases.isEmpty();
    }

    @VisibleForTesting
    static List<Set<PlanFragmentId>> extractPhases(Collection<PlanFragment> fragments)
    {
        // Build a graph where the plan fragments are vertexes and the edges represent
        // a before -> after relationship.  For example, a join hash build has an edge
        // to the join probe.
        Graph<PlanFragmentId, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        fragments.forEach(fragment -> graph.addVertex(fragment.getId()));

        Visitor visitor = new Visitor(fragments, graph);
        for (PlanFragment fragment : fragments) {
            visitor.processFragment(fragment.getId());
        }

        // Computes all the strongly connected components of the directed graph.
        // These are the "phases" which hold the set of fragments that must be started
        // at the same time to avoid deadlock.
        List<Set<PlanFragmentId>> components = new KosarajuStrongConnectivityInspector<>(graph).stronglyConnectedSets();

        Map<PlanFragmentId, Set<PlanFragmentId>> componentMembership = new HashMap<>();
        for (Set<PlanFragmentId> component : components) {
            for (PlanFragmentId planFragmentId : component) {
                componentMembership.put(planFragmentId, component);
            }
        }

        // build graph of components (phases)
        Graph<Set<PlanFragmentId>, DefaultEdge> componentGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        components.forEach(componentGraph::addVertex);
        for (DefaultEdge edge : graph.edgeSet()) {
            PlanFragmentId source = graph.getEdgeSource(edge);
            PlanFragmentId target = graph.getEdgeTarget(edge);

            Set<PlanFragmentId> from = componentMembership.get(source);
            Set<PlanFragmentId> to = componentMembership.get(target);
            if (!from.equals(to)) { // the topological order iterator below doesn't include vertices that have self-edges, so don't add them
                componentGraph.addEdge(from, to);
            }
        }

        List<Set<PlanFragmentId>> schedulePhases = ImmutableList.copyOf(new TopologicalOrderIterator<>(componentGraph));
        return schedulePhases;
    }

    private static class Visitor
            extends InternalPlanVisitor<Set<PlanFragmentId>, PlanFragmentId>
    {
        private final Map<PlanFragmentId, PlanFragment> fragments;
        private final Graph<PlanFragmentId, DefaultEdge> graph;
        private final Map<PlanFragmentId, Set<PlanFragmentId>> fragmentSources = new HashMap<>();

        public Visitor(Collection<PlanFragment> fragments, Graph<PlanFragmentId, DefaultEdge> graph)
        {
            this.fragments = fragments.stream()
                    .collect(toImmutableMap(PlanFragment::getId, identity()));
            this.graph = graph;
        }

        public Set<PlanFragmentId> processFragment(PlanFragmentId planFragmentId)
        {
            if (fragmentSources.containsKey(planFragmentId)) {
                return fragmentSources.get(planFragmentId);
            }

            Set<PlanFragmentId> fragment = processFragment(fragments.get(planFragmentId));
            fragmentSources.put(planFragmentId, fragment);
            return fragment;
        }

        private Set<PlanFragmentId> processFragment(PlanFragment fragment)
        {
            Set<PlanFragmentId> sources = fragment.getRoot().accept(this, fragment.getId());
            return ImmutableSet.<PlanFragmentId>builder().add(fragment.getId()).addAll(sources).build();
        }

        @Override
        public Set<PlanFragmentId> visitJoin(JoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitStarJoin(StarJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processStarJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitSpatialJoin(SpatialJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitSemiJoin(SemiJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getFilteringSource(), node.getSource(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitIndexJoin(IndexJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getIndexSource(), node.getProbeSource(), currentFragmentId);
        }

        @Override
        public Set<PlanFragmentId> visitMergeJoin(MergeJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(node.getRight(), node.getLeft(), currentFragmentId);
        }

        private Set<PlanFragmentId> processJoin(PlanNode build, PlanNode probe, PlanFragmentId currentFragmentId)
        {
            Set<PlanFragmentId> buildSources = build.accept(this, currentFragmentId);
            Set<PlanFragmentId> probeSources = probe.accept(this, currentFragmentId);

            for (PlanFragmentId buildSource : buildSources) {
                for (PlanFragmentId probeSource : probeSources) {
                    graph.addEdge(buildSource, probeSource);
                }
            }

            return ImmutableSet.<PlanFragmentId>builder()
                    .addAll(buildSources)
                    .addAll(probeSources)
                    .build();
        }

        private Set<PlanFragmentId> processStarJoin(List<PlanNode> build, PlanNode probe, PlanFragmentId currentFragmentId)
        {
            List<Set<PlanFragmentId>> buildSourcesList = build.stream().map(x -> x.accept(this, currentFragmentId)).collect(toImmutableList());
            Set<PlanFragmentId> probeSources = probe.accept(this, currentFragmentId);

            for (Set<PlanFragmentId> buildSources : buildSourcesList) {
                for (PlanFragmentId buildSource : buildSources) {
                    for (PlanFragmentId probeSource : probeSources) {
                        graph.addEdge(buildSource, probeSource);
                    }
                }
            }

            ImmutableSet.Builder<PlanFragmentId> result = ImmutableSet.builder();
            buildSourcesList.forEach(x -> result.addAll(x));
            result.addAll(probeSources);
            return result.build();
        }

        @Override
        public Set<PlanFragmentId> visitRemoteSource(RemoteSourceNode node, PlanFragmentId currentFragmentId)
        {
            ImmutableSet.Builder<PlanFragmentId> sources = ImmutableSet.builder();

            Set<PlanFragmentId> previousFragmentSources = ImmutableSet.of();
            for (PlanFragmentId remoteFragment : node.getSourceFragmentIds()) {
                // this current fragment depends on the remote fragment
                graph.addEdge(currentFragmentId, remoteFragment);

                // get all sources for the remote fragment
                Set<PlanFragmentId> remoteFragmentSources = processFragment(remoteFragment);
                sources.addAll(remoteFragmentSources);

                // For UNION there can be multiple sources.
                // Link the previous source to the current source, so we only
                // schedule one at a time.
                addEdges(previousFragmentSources, remoteFragmentSources);

                previousFragmentSources = remoteFragmentSources;
            }

            return sources.build();
        }

        @Override
        public Set<PlanFragmentId> visitExchange(ExchangeNode node, PlanFragmentId currentFragmentId)
        {
            checkArgument(node.getScope().isLocal(), "Only local exchanges are supported in the phased execution scheduler");
            ImmutableSet.Builder<PlanFragmentId> allSources = ImmutableSet.builder();

            // Link the source fragments together, so we only schedule one at a time.
            Set<PlanFragmentId> previousSources = ImmutableSet.of();
            for (PlanNode subPlanNode : node.getSources()) {
                Set<PlanFragmentId> currentSources = subPlanNode.accept(this, currentFragmentId);
                allSources.addAll(currentSources);

                addEdges(previousSources, currentSources);

                previousSources = currentSources;
            }

            return allSources.build();
        }

        @Override
        public Set<PlanFragmentId> visitUnion(UnionNode node, PlanFragmentId currentFragmentId)
        {
            ImmutableSet.Builder<PlanFragmentId> allSources = ImmutableSet.builder();

            // Link the source fragments together, so we only schedule one at a time.
            Set<PlanFragmentId> previousSources = ImmutableSet.of();
            for (PlanNode subPlanNode : node.getSources()) {
                Set<PlanFragmentId> currentSources = subPlanNode.accept(this, currentFragmentId);
                allSources.addAll(currentSources);

                addEdges(previousSources, currentSources);

                previousSources = currentSources;
            }

            return allSources.build();
        }

        @Override
        public Set<PlanFragmentId> visitPlan(PlanNode node, PlanFragmentId currentFragmentId)
        {
            List<PlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return ImmutableSet.of(currentFragmentId);
            }
            if (sources.size() == 1) {
                return sources.get(0).accept(this, currentFragmentId);
            }
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private void addEdges(Set<PlanFragmentId> sourceFragments, Set<PlanFragmentId> targetFragments)
        {
            for (PlanFragmentId targetFragment : targetFragments) {
                for (PlanFragmentId sourceFragment : sourceFragments) {
                    graph.addEdge(sourceFragment, targetFragment);
                }
            }
        }
    }
}
