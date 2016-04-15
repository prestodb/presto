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

import com.facebook.presto.execution.resourceGroups.ResourceGroupSelector;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Provider;
import com.google.inject.Provides;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryQueueRuleFactory
        implements Provider<Map<String, ? extends ResourceGroupSelector>>
{
    private final Map<String, ? extends ResourceGroupSelector> selectors;

    @Inject
    public QueryQueueRuleFactory(QueryManagerConfig config, ObjectMapper mapper)
    {
        requireNonNull(config, "config is null");

        ImmutableMap.Builder<String, QueryQueueRule> rules = ImmutableMap.builder();
        if (config.getQueueConfigFile() == null) {
            QueryQueueDefinition global = new QueryQueueDefinition("global",
                    config.getMaxConcurrentQueries(),
                    config.getMaxQueuedQueries(),
                    config.getQueueMaxMemory(),
                    config.getQueueMaxCpuTime(),
                    config.getQueueMaxQueryCpuTime(),
                    config.getQueueRuntimeCap(),
                    config.getQueueQueuedTimeCap(),
                    config.getQueueIsPublic()
                    );
            rules.put("global", new QueryQueueRule(global, ImmutableSet.<String>of(), ImmutableMap.of()));
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
                definitions.put(queue.getKey(), new QueryQueueDefinition(
                        queue.getKey(),
                        queue.getValue().getMaxConcurrent(),
                        queue.getValue().getMaxQueued(),
                        queue.getValue().getMaxMemory(),
                        queue.getValue().getMaxCpuTime(),
                        queue.getValue().getMaxQueryCpuTime(),
                        queue.getValue().getRuntimeCap(),
                        queue.getValue().getQueuedTimeCap(),
                        queue.getValue().getIsPublic())
                );
            }

            for (RuleSpec rule : managerSpec.getRules()) {
                rules.put(rule.getQueueName(), QueryQueueRule.createRule(definitions.get(rule.getQueueName()), rule.getUserNames(), ImmutableMap.of()));
            }
        }
        this.selectors = rules.build();
    }

    @Override
    @Provides
    public Map<String, ? extends ResourceGroupSelector> get()
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
            this.queues = getLeafQueues(requireNonNull(queues, "queues is null"));
            this.rules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        }

        private static ImmutableMap<String, QueueSpec> getLeafQueues(Map<String, QueueSpec> queues)
        {
            ImmutableMap.Builder<String, QueueSpec> builder = ImmutableMap.builder();
            for (Map.Entry<String, QueueSpec> entry : queues.entrySet()) {
                if (entry.getKey().contains(".")) {
                    throw new IllegalArgumentException("Queue name cannot contain \".\"'s since \".\" is the delimiter");
                }
                LinkedList<Map.Entry<String, QueueSpec>> queue = new LinkedList<>();
                queue.add(entry);
                while (!queue.isEmpty()) {
                    Map.Entry<String, QueueSpec> subEntry = queue.poll();
                    if (subEntry.getValue().getSubGroups().isEmpty()) {
                        builder.put(subEntry);
                    }
                    else {
                        for (Map.Entry<String, QueueSpec> sub : subEntry.getValue().getSubGroups().entrySet()) {
                            if (sub.getKey().contains(".")) {
                                throw new IllegalArgumentException("Queue name cannot contain \".\"'s since \".\" is the delimiter");
                            }
                            Map.Entry<String, QueueSpec> subEntryWithPath = new SimpleEntry<>(
                                    subEntry.getKey() + "." + sub.getKey(), sub.getValue());
                            queue.add(subEntryWithPath);
                        }
                    }
                }
            }
            return builder.build();
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
        private final DataSize maxMemory;
        private final Duration maxCpuTime;
        private final Duration maxQueryCpuTime;
        private final Duration runtimeCap;
        private final Duration queuedTimeCap;
        private final boolean isPublic;
        private final ImmutableMap<String, QueueSpec> subGroups;
        @JsonCreator
        public QueueSpec(
                @JsonProperty("maxQueued") int maxQueued,
                @JsonProperty("maxConcurrent") int maxConcurrent,
                @JsonProperty("maxMemory") DataSize maxMemory,
                @JsonProperty("maxCpuTime") Duration maxCpuTime,
                @JsonProperty("maxQueryCpuTime") Duration maxQueryCpuTime,
                @JsonProperty("runtimeCap") Duration runtimeCap,
                @JsonProperty("queuedTimeCap") Duration queuedTimeCap,
                @JsonProperty("isPublic") boolean isPublic,
                @JsonProperty("subGroups") Map<String, QueueSpec> subGroups
                )
        {
            this.maxQueued = maxQueued;
            this.maxConcurrent = maxConcurrent;
            this.maxMemory = maxMemory;
            this.maxCpuTime = maxCpuTime;
            this.maxQueryCpuTime = maxQueryCpuTime;
            this.runtimeCap = runtimeCap;
            this.queuedTimeCap = queuedTimeCap;
            this.isPublic = isPublic;
            this.subGroups = ImmutableMap.copyOf(subGroups);
        }

        public int getMaxQueued()
        {
            return maxQueued;
        }

        public int getMaxConcurrent()
        {
            return maxConcurrent;
        }

        public DataSize getMaxMemory()
        {
            return maxMemory;
        }

        public Duration getMaxCpuTime()
        {
            return maxCpuTime;
        }

        public Duration getMaxQueryCpuTime()
        {
            return maxQueryCpuTime;
        }

        public Duration getRuntimeCap()
        {
            return runtimeCap;
        }

        public Duration getQueuedTimeCap()
        {
            return queuedTimeCap;
        }

        public boolean getIsPublic()
        {
            return isPublic;
        }

        public ImmutableMap<String, QueueSpec> getSubGroups()
        {
            return subGroups;
        }
    }

    public static class RuleSpec
    {
        @Nullable
        private final ImmutableSet<String> userNames;
        @Nullable
        private final String queueName;
        private final Map<String, Pattern> sessionPropertyRegexes = new HashMap<>();

        @JsonCreator
        public RuleSpec(
                @JsonProperty("queue") @Nullable String queueName,
                @JsonProperty("users") @Nullable List<String> userNames)
        {
            this.queueName = queueName;
            this.userNames = ImmutableSet.copyOf(userNames);
        }

        @JsonAnySetter
        public void setSessionProperty(String property, Pattern value)
        {
            checkArgument(property.startsWith("session."), "Unrecognized property: %s", property);
            sessionPropertyRegexes.put(property.substring("session.".length(), property.length()), value);
        }

        @Nullable
        public ImmutableSet<String> getUserNames()
        {
            return userNames;
        }

        @Nullable
        public String getQueueName()
        {
            return queueName;
        }

        public Map<String, Pattern> getSessionPropertyRegexes()
        {
            return ImmutableMap.copyOf(sessionPropertyRegexes);
        }
    }
}
