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
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.google.inject.Provides;
import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryQueueRuleFactory
        implements Provider<List<QueryQueueRule>>
{
    private final List<QueryQueueRule> selectors;

    @Inject
    public QueryQueueRuleFactory(QueryManagerConfig config, ObjectMapper mapper)
    {
        requireNonNull(config, "config is null");

        ImmutableList.Builder<QueryQueueRule> rules = ImmutableList.builder();
        if (config.getQueueConfigFile() == null) {
            QueryQueueDefinition global = new QueryQueueDefinition("global", config.getMaxConcurrentQueries(), config.getMaxQueuedQueries());
            rules.add(new QueryQueueRule(null, null, ImmutableMap.of(), ImmutableList.of(global)));
        }
        else {
            File file = new File(config.getQueueConfigFile());
            ManagerSpec managerSpec;
            try {
                managerSpec = mapper.readValue(file, ManagerSpec.class);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            Map<String, QueryQueueDefinition> definitions = new HashMap<>();
            for (Map.Entry<String, QueueSpec> queue : managerSpec.getQueues().entrySet()) {
                definitions.put(queue.getKey(), new QueryQueueDefinition(queue.getKey(), queue.getValue().getMaxConcurrent(), queue.getValue().getMaxQueued()));
            }

            for (RuleSpec rule : managerSpec.getRules()) {
                rules.add(QueryQueueRule.createRule(rule.getUserRegex(), rule.getSourceRegex(), rule.getSessionPropertyRegexes(), rule.getQueues(), definitions));
            }
        }
        checkIsTree(rules.build());
        this.selectors = rules.build();
    }

    private static void checkIsTree(List<QueryQueueRule> rules)
    {
        DirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        for (QueryQueueRule rule : rules) {
            String lastQueueName = null;
            for (QueryQueueDefinition queue : rule.getQueues()) {
                String currentQueueName = queue.getTemplate();
                graph.addVertex(currentQueueName);
                if (lastQueueName != null) {
                    graph.addEdge(lastQueueName, currentQueueName);
                }
                lastQueueName = currentQueueName;
            }
        }

        for (String vertex : graph.vertexSet()) {
            if (graph.outDegreeOf(vertex) > 1) {
                List<String> targets = graph.outgoingEdgesOf(vertex).stream()
                        .map(graph::getEdgeTarget)
                        .collect(Collectors.toList());
                throw new IllegalArgumentException(format("Queues must form a tree. Queue %s feeds into %s", vertex, targets));
            }
        }

        List<String> shortestCycle = shortestCycle(graph);

        if (shortestCycle != null) {
            String s = Joiner.on(", ").join(shortestCycle);
            throw new IllegalArgumentException(format("Queues must not contain a cycle. The shortest cycle found is [%s]", s));
        }
    }

    private static List<String> shortestCycle(DirectedGraph<String, DefaultEdge> graph)
    {
        FloydWarshallShortestPaths<String, DefaultEdge> floyd = new FloydWarshallShortestPaths<>(graph);
        int minDistance = Integer.MAX_VALUE;
        String minSource = null;
        String minDestination = null;
        for (DefaultEdge edge : graph.edgeSet()) {
            String src = graph.getEdgeSource(edge);
            String dst = graph.getEdgeTarget(edge);
            int dist = (int) Math.round(floyd.shortestDistance(dst, src)); // from dst to src
            if (dist < 0) {
                continue;
            }
            if (dist < minDistance) {
                minDistance = dist;
                minSource = src;
                minDestination = dst;
            }
        }
        if (minSource == null) {
            return null;
        }
        GraphPath<String, DefaultEdge> shortestPath = floyd.getShortestPath(minDestination, minSource);
        List<String> pathVertexList = Graphs.getPathVertexList(shortestPath);
        // note: pathVertexList will be [a, a] instead of [a] when the shortest path is a loop edge
        if (!Objects.equals(shortestPath.getStartVertex(), shortestPath.getEndVertex())) {
            pathVertexList.add(pathVertexList.get(0));
        }
        return pathVertexList;
    }

    @Override
    @Provides
    public List<QueryQueueRule> get()
    {
        return selectors;
    }

    public static class ManagerSpec
    {
        private final Map<String, QueueSpec> queues;
        private final List<RuleSpec> rules;

        @JsonCreator
        public ManagerSpec(
                @JsonProperty("queues") Map<String, QueueSpec> queues,
                @JsonProperty("rules") List<RuleSpec> rules)
        {
            this.queues = ImmutableMap.copyOf(requireNonNull(queues, "queues is null"));
            this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        }

        public Map<String, QueueSpec> getQueues()
        {
            return queues;
        }

        public List<RuleSpec> getRules()
        {
            return rules;
        }
    }

    public static class QueueSpec
    {
        private final int maxQueued;
        private final int maxConcurrent;

        @JsonCreator
        public QueueSpec(
                @JsonProperty("maxQueued") int maxQueued,
                @JsonProperty("maxConcurrent") int maxConcurrent)
        {
            this.maxQueued = maxQueued;
            this.maxConcurrent = maxConcurrent;
        }

        public int getMaxQueued()
        {
            return maxQueued;
        }

        public int getMaxConcurrent()
        {
            return maxConcurrent;
        }
    }

    public static class RuleSpec
    {
        @Nullable
        private final Pattern userRegex;
        @Nullable
        private final Pattern sourceRegex;
        private final Map<String, Pattern> sessionPropertyRegexes = new HashMap<>();
        private final List<String> queues;

        @JsonCreator
        public RuleSpec(
                @JsonProperty("user") @Nullable Pattern userRegex,
                @JsonProperty("source") @Nullable Pattern sourceRegex,
                @JsonProperty("queues") List<String> queues)
        {
            this.userRegex = userRegex;
            this.sourceRegex = sourceRegex;
            this.queues = ImmutableList.copyOf(queues);
        }

        @JsonAnySetter
        public void setSessionProperty(String property, Pattern value)
        {
            checkArgument(property.startsWith("session."), "Unrecognized property: %s", property);
            sessionPropertyRegexes.put(property.substring("session.".length(), property.length()), value);
        }

        @Nullable
        public Pattern getUserRegex()
        {
            return userRegex;
        }

        @Nullable
        public Pattern getSourceRegex()
        {
            return sourceRegex;
        }

        public Map<String, Pattern> getSessionPropertyRegexes()
        {
            return ImmutableMap.copyOf(sessionPropertyRegexes);
        }

        public List<String> getQueues()
        {
            return queues;
        }
    }
}
