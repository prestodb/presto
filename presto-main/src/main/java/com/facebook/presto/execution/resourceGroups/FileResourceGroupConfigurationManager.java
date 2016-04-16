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

import com.facebook.presto.SessionRepresentation;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;

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
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        implements ResourceGroupConfigurationManager, Provider<List<? extends ResourceGroupSelector>>
{
    private final List<ResourceGroupSpec> rootGroups;
    private final List<? extends ResourceGroupSelector> selectors;

    @Inject
    public FileResourceGroupConfigurationManager(ResourceGroupConfig config, JsonCodec<ManagerSpec> codec)
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
            selectors.add(new StaticSelector(spec.getUserRegex(), spec.getSourceRegex(), spec.getSessionPropertyRegexes(), spec.getGroup()));
        }
        this.selectors = selectors.build();
    }

    @Override
    public void configure(ConfigurableResourceGroup group, SessionRepresentation session)
    {
        List<ResourceGroupSpec> candidates = rootGroups;
        List<String> segments = group.getId().getSegments();
        ResourceGroupSpec match = null;
        for (int i = 0; i < segments.size(); i++) {
            List<ResourceGroupSpec> nextCandidates = null;
            ResourceGroupSpec nextCandidatesParent = null;
            for (ResourceGroupSpec candidate : candidates) {
                if (candidate.getName().expandTemplate(session).equals(segments.get(i))) {
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
        group.setSoftMemoryLimit(match.getSoftMemoryLimit());
        group.setMaxQueuedQueries(match.getMaxQueued());
        group.setMaxRunningQueries(match.getMaxRunning());
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
        private final ResourceGroupNameTemplate name;
        private final DataSize softMemoryLimit;
        private final int maxQueued;
        private final int maxRunning;
        private final List<ResourceGroupSpec> subGroups;

        @JsonCreator
        public ResourceGroupSpec(
                @JsonProperty("name") ResourceGroupNameTemplate name,
                @JsonProperty("softMemoryLimit") DataSize softMemoryLimit,
                @JsonProperty("maxQueued") int maxQueued,
                @JsonProperty("maxRunning") int maxRunning,
                @JsonProperty("subGroups") Optional<List<ResourceGroupSpec>> subGroups)
        {
            this.name = requireNonNull(name, "name is null");
            checkArgument(maxQueued >= 0, "maxQueued is negative");
            this.maxQueued = maxQueued;
            checkArgument(maxRunning >= 0, "maxRunning is negative");
            this.maxRunning = maxRunning;
            this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
            this.subGroups = ImmutableList.copyOf(requireNonNull(subGroups, "subGroups is null").orElse(ImmutableList.of()));
            Set<ResourceGroupNameTemplate> names = new HashSet<>();
            for (ResourceGroupSpec subGroup : this.subGroups) {
                checkArgument(!names.contains(subGroup.getName()), "Duplicated sub group: %s", subGroup.getName());
                names.add(subGroup.getName());
            }
        }

        public DataSize getSoftMemoryLimit()
        {
            return softMemoryLimit;
        }

        public int getMaxQueued()
        {
            return maxQueued;
        }

        public int getMaxRunning()
        {
            return maxRunning;
        }

        public ResourceGroupNameTemplate getName()
        {
            return name;
        }

        public List<ResourceGroupSpec> getSubGroups()
        {
            return subGroups;
        }
    }

    public static class SelectorSpec
    {
        private final Optional<Pattern> userRegex;
        private final Optional<Pattern> sourceRegex;
        private final Map<String, Pattern> sessionPropertyRegexes = new HashMap<>();
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

        @JsonAnySetter
        public void setSessionProperty(String property, Pattern value)
        {
            checkArgument(property.startsWith("session."), "Unrecognized property: %s", property);
            sessionPropertyRegexes.put(property.substring("session.".length(), property.length()), value);
        }

        public Optional<Pattern> getUserRegex()
        {
            return userRegex;
        }

        public Optional<Pattern> getSourceRegex()
        {
            return sourceRegex;
        }

        public Map<String, Pattern> getSessionPropertyRegexes()
        {
            return ImmutableMap.copyOf(sessionPropertyRegexes);
        }

        public ResourceGroupIdTemplate getGroup()
        {
            return group;
        }
    }
}
