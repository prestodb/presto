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

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jgrapht.DirectedGraph;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.FloydWarshallShortestPaths;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

import static com.facebook.presto.execution.QueuedExecution.createQueuedExecution;
import static com.facebook.presto.spi.StandardErrorCode.USER_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SqlQueryQueueManager
        implements QueryQueueManager
{
    private final ConcurrentMap<QueueKey, QueryQueue> queryQueues = new ConcurrentHashMap<>();
    private final List<QueryQueueRule> rules;
    private final MBeanExporter mbeanExporter;

    @Inject
    public SqlQueryQueueManager(QueryManagerConfig config, ObjectMapper mapper, MBeanExporter mbeanExporter)
    {
        requireNonNull(config, "config is null");
        this.mbeanExporter = requireNonNull(mbeanExporter, "mbeanExporter is null");

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
        this.rules = rules.build();
        checkIsDAG(this.rules);
    }

    private static void checkIsDAG(List<QueryQueueRule> rules)
    {
        DirectedPseudograph<String, DefaultEdge> graph = new DirectedPseudograph<>(DefaultEdge.class);
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
    public boolean submit(QueryExecution queryExecution, Executor executor, SqlQueryManagerStats stats)
    {
        List<QueryQueue> queues = selectQueues(queryExecution.getSession(), executor);

        for (QueryQueue queue : queues) {
            if (!queue.reserve(queryExecution)) {
                // Reject query if we couldn't acquire a permit to enter the queue.
                // The permits will be released when this query fails.
                return false;
            }
        }

        queues.get(0).enqueue(createQueuedExecution(queryExecution, queues.subList(1, queues.size()), executor, stats));
        return true;
    }

    // Queues returned have already been created and added queryQueues
    private List<QueryQueue> selectQueues(Session session, Executor executor)
    {
        for (QueryQueueRule rule : rules) {
            List<QueryQueueDefinition> definitions = rule.match(session);
            if (definitions != null) {
                return getOrCreateQueues(session, executor, definitions);
            }
        }
        throw new PrestoException(USER_ERROR, "Query did not match any queuing rule");
    }

    private List<QueryQueue> getOrCreateQueues(Session session, Executor executor, List<QueryQueueDefinition> definitions)
    {
        ImmutableList.Builder<QueryQueue> queues = ImmutableList.builder();
        for (QueryQueueDefinition definition : definitions) {
            String expandedName = definition.getExpandedTemplate(session);
            QueueKey key = new QueueKey(definition, expandedName);
            if (!queryQueues.containsKey(key)) {
                QueryQueue queue = new QueryQueue(executor, definition.getMaxQueued(), definition.getMaxConcurrent());
                if (queryQueues.putIfAbsent(key, queue) == null) {
                    // Export the mbean, after checking for races
                    String objectName = ObjectNames.builder(QueryQueue.class, definition.getTemplate()).withProperty("expansion", expandedName).build();
                    mbeanExporter.export(objectName, queue);
                }
            }
            queues.add(queryQueues.get(key));
        }
        return queues.build();
    }

    @PreDestroy
    public void destroy()
    {
        for (QueueKey key : queryQueues.keySet()) {
            String objectName = ObjectNames.builder(QueryQueue.class, key.getQueue().getTemplate()).withProperty("expansion", key.getName()).build();
            mbeanExporter.unexport(objectName);
        }
    }

    private static class QueueKey
    {
        private final QueryQueueDefinition queue;
        private final String name;

        private QueueKey(QueryQueueDefinition queue, String name)
        {
            this.queue = requireNonNull(queue, "queue is null");
            this.name = requireNonNull(name, "name is null");
        }

        public QueryQueueDefinition getQueue()
        {
            return queue;
        }

        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            QueueKey queueKey = (QueueKey) other;

            return Objects.equals(name, queueKey.name) && Objects.equals(queue.getTemplate(), queueKey.queue.getTemplate());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queue.getTemplate(), name);
        }
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
