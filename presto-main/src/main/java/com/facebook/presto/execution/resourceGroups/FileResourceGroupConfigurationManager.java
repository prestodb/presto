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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.resourceGroups.ResourceGroup.SubGroupSchedulingPolicy;
import com.facebook.presto.memory.ClusterMemoryPoolManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        implements ResourceGroupConfigurationManager, Provider<List<? extends ResourceGroupSelector>>
{
    private final List<ResourceGroupSpec> rootGroups;
    private final List<? extends ResourceGroupSelector> selectors;

    @GuardedBy("generalPoolMemoryFraction")
    private final Map<ConfigurableResourceGroup, Double> generalPoolMemoryFraction = new HashMap<>();
    @GuardedBy("generalPoolMemoryFraction")
    private long generalPoolBytes;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, ResourceGroupConfig config, JsonCodec<ManagerSpec> codec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(codec, "codec is null");

        ManagerSpec managerSpec;
        try {
            managerSpec = codec.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        this.rootGroups = managerSpec.getRootGroups();

        ImmutableList.Builder<ResourceGroupSelector> selectors = ImmutableList.builder();
        for (SelectorSpec spec : managerSpec.getSelectors()) {
            selectors.add(new StaticSelector(spec.getUserRegex(), spec.getSourceRegex(), spec.getGroup()));
        }
        this.selectors = selectors.build();

        memoryPoolManager.addChangeListener(GENERAL_POOL, poolInfo -> {
            synchronized (generalPoolMemoryFraction) {
                for (Map.Entry<ConfigurableResourceGroup, Double> entry : generalPoolMemoryFraction.entrySet()) {
                    double bytes = poolInfo.getMaxBytes() * entry.getValue();
                    entry.getKey().setSoftMemoryLimit(new DataSize(bytes, BYTE));
                }
                generalPoolBytes = poolInfo.getMaxBytes();
            }
        });
    }

    @Override
    public void configure(ConfigurableResourceGroup group, SelectionContext context)
    {
        List<ResourceGroupSpec> candidates = rootGroups;
        List<String> segments = group.getId().getSegments();
        ResourceGroupSpec match = null;
        for (int i = 0; i < segments.size(); i++) {
            List<ResourceGroupSpec> nextCandidates = null;
            ResourceGroupSpec nextCandidatesParent = null;
            for (ResourceGroupSpec candidate : candidates) {
                if (candidate.getName().expandTemplate(context).equals(segments.get(i))) {
                    if (i == segments.size() - 1) {
                        if (match != null) {
                            throw new IllegalStateException(format("Ambiguous configuration for %s. Matches %s and %s", group.getId(), match.getName(), candidate.getName()));
                        }
                        match = candidate;
                    }
                    else {
                        if (nextCandidatesParent != null) {
                            throw new IllegalStateException(format("Ambiguous configuration for %s. Matches %s and %s", group.getId(), nextCandidatesParent.getName(), candidate.getName()));
                        }
                        nextCandidates = candidate.getSubGroups();
                        nextCandidatesParent = candidate;
                    }
                }
            }
            if (nextCandidates == null) {
                break;
            }
            candidates = nextCandidates;
        }
        checkState(match != null, "No matching configuration found for: %s", group.getId());
        if (match.getSoftMemoryLimit().isPresent()) {
            group.setSoftMemoryLimit(match.getSoftMemoryLimit().get());
        }
        else {
            synchronized (generalPoolMemoryFraction) {
                double fraction = match.getSoftMemoryLimitFraction().get();
                generalPoolMemoryFraction.put(group, fraction);
                group.setSoftMemoryLimit(new DataSize(generalPoolBytes * fraction, BYTE));
            }
        }
        group.setMaxQueuedQueries(match.getMaxQueued());
        group.setMaxRunningQueries(match.getMaxRunning());
        if (match.getSchedulingPolicy().isPresent()) {
            group.setSchedulingPolicy(match.getSchedulingPolicy().get());
        }
        if (match.getSchedulingWeight().isPresent()) {
            group.setSchedulingWeight(match.getSchedulingWeight().get());
        }
        if (match.getJmxExport().isPresent()) {
            group.setJmxExport(match.getJmxExport().get());
        }
    }

    @Override
    public List<? extends ResourceGroupSelector> get()
    {
        return selectors;
    }

    public static class ManagerSpec
    {
        private final List<ResourceGroupSpec> rootGroups;
        private final List<SelectorSpec> selectors;

        @JsonCreator
        public ManagerSpec(
                @JsonProperty("rootGroups") List<ResourceGroupSpec> rootGroups,
                @JsonProperty("selectors") List<SelectorSpec> selectors)
        {
            this.rootGroups = ImmutableList.copyOf(requireNonNull(rootGroups, "rootGroups is null"));
            this.selectors = ImmutableList.copyOf(requireNonNull(selectors, "selectors is null"));
            Set<ResourceGroupNameTemplate> names = new HashSet<>();
            for (ResourceGroupSpec group : rootGroups) {
                checkArgument(!names.contains(group.getName()), "Duplicated root group: %s", group.getName());
                names.add(group.getName());
            }
        }

        public List<ResourceGroupSpec> getRootGroups()
        {
            return rootGroups;
        }

        public List<SelectorSpec> getSelectors()
        {
            return selectors;
        }
    }

    public static class ResourceGroupSpec
    {
        private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d{1,3}(:?\\.\\d+)?)%");

        private final ResourceGroupNameTemplate name;
        private final Optional<DataSize> softMemoryLimit;
        private final Optional<Double> softMemoryLimitFraction;
        private final int maxQueued;
        private final int maxRunning;
        private final Optional<SubGroupSchedulingPolicy> schedulingPolicy;
        private final Optional<Integer> schedulingWeight;
        private final List<ResourceGroupSpec> subGroups;
        private final Optional<Boolean> jmxExport;

        @JsonCreator
        public ResourceGroupSpec(
                @JsonProperty("name") ResourceGroupNameTemplate name,
                @JsonProperty("softMemoryLimit") String softMemoryLimit,
                @JsonProperty("maxQueued") int maxQueued,
                @JsonProperty("maxRunning") int maxRunning,
                @JsonProperty("schedulingPolicy") Optional<String> schedulingPolicy,
                @JsonProperty("schedulingWeight") Optional<Integer> schedulingWeight,
                @JsonProperty("subGroups") Optional<List<ResourceGroupSpec>> subGroups,
                @JsonProperty("jmxExport") Optional<Boolean> jmxExport)
        {
            this.jmxExport = requireNonNull(jmxExport, "jmxExport is null");
            this.name = requireNonNull(name, "name is null");
            checkArgument(maxQueued >= 0, "maxQueued is negative");
            this.maxQueued = maxQueued;
            checkArgument(maxRunning >= 0, "maxRunning is negative");
            this.maxRunning = maxRunning;
            this.schedulingPolicy = requireNonNull(schedulingPolicy, "schedulingPolicy is null").map(value -> SubGroupSchedulingPolicy.valueOf(value.toUpperCase()));
            this.schedulingWeight = requireNonNull(schedulingWeight, "schedulingWeight is null");
            requireNonNull(softMemoryLimit, "softMemoryLimit is null");
            Optional<DataSize> absoluteSize;
            Optional<Double> fraction;
            Matcher matcher = PERCENT_PATTERN.matcher(softMemoryLimit);
            if (matcher.matches()) {
                absoluteSize = Optional.empty();
                fraction = Optional.of(Double.parseDouble(matcher.group(1)) / 100.0);
            }
            else {
                absoluteSize = Optional.of(DataSize.valueOf(softMemoryLimit));
                fraction = Optional.empty();
            }
            this.softMemoryLimit = absoluteSize;
            this.softMemoryLimitFraction = fraction;
            this.subGroups = ImmutableList.copyOf(requireNonNull(subGroups, "subGroups is null").orElse(ImmutableList.of()));
            Set<ResourceGroupNameTemplate> names = new HashSet<>();
            for (ResourceGroupSpec subGroup : this.subGroups) {
                checkArgument(!names.contains(subGroup.getName()), "Duplicated sub group: %s", subGroup.getName());
                names.add(subGroup.getName());
            }
        }

        public Optional<DataSize> getSoftMemoryLimit()
        {
            return softMemoryLimit;
        }

        public Optional<Double> getSoftMemoryLimitFraction()
        {
            return softMemoryLimitFraction;
        }

        public int getMaxQueued()
        {
            return maxQueued;
        }

        public int getMaxRunning()
        {
            return maxRunning;
        }

        public Optional<SubGroupSchedulingPolicy> getSchedulingPolicy()
        {
            return schedulingPolicy;
        }

        public Optional<Integer> getSchedulingWeight()
        {
            return schedulingWeight;
        }

        public ResourceGroupNameTemplate getName()
        {
            return name;
        }

        public List<ResourceGroupSpec> getSubGroups()
        {
            return subGroups;
        }

        public Optional<Boolean> getJmxExport()
        {
            return jmxExport;
        }
    }

    public static class SelectorSpec
    {
        private final Optional<Pattern> userRegex;
        private final Optional<Pattern> sourceRegex;
        private final ResourceGroupIdTemplate group;

        @JsonCreator
        public SelectorSpec(
                @JsonProperty("user") Optional<Pattern> userRegex,
                @JsonProperty("source") Optional<Pattern> sourceRegex,
                @JsonProperty("group") ResourceGroupIdTemplate group)
        {
            this.userRegex = requireNonNull(userRegex, "userRegex is null");
            this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
            this.group = requireNonNull(group, "group is null");
        }

        public Optional<Pattern> getUserRegex()
        {
            return userRegex;
        }

        public Optional<Pattern> getSourceRegex()
        {
            return sourceRegex;
        }

        public ResourceGroupIdTemplate getGroup()
        {
            return group;
        }
    }
}
