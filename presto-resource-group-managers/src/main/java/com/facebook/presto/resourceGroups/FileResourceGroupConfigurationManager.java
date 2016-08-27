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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        implements ResourceGroupConfigurationManager
{
    private final List<ResourceGroupSpec> rootGroups;
    private final List<ResourceGroupSelector> selectors;
    private final Optional<Duration> cpuQuotaPeriodMillis;

    @GuardedBy("generalPoolMemoryFraction")
    private final Map<ResourceGroup, Double> generalPoolMemoryFraction = new HashMap<>();
    @GuardedBy("generalPoolMemoryFraction")
    private long generalPoolBytes;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, FileResourceGroupConfig config, JsonCodec<ManagerSpec> codec)
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
        this.cpuQuotaPeriodMillis = managerSpec.getCpuQuotaPeriod();

        Queue<ResourceGroupSpec> groups = new LinkedList<>(rootGroups);
        while (!groups.isEmpty()) {
            ResourceGroupSpec group = groups.poll();
            groups.addAll(group.getSubGroups());
            if (group.getSoftCpuLimit().isPresent() || group.getHardCpuLimit().isPresent()) {
                checkArgument(managerSpec.getCpuQuotaPeriod().isPresent(), "cpuQuotaPeriod must be specified to use cpu limits on group: %s", group.getName());
            }
            if (group.getSoftCpuLimit().isPresent()) {
                checkArgument(group.getHardCpuLimit().isPresent(), "Must specify hard CPU limit in addition to soft limit");
                checkArgument(group.getSoftCpuLimit().get().compareTo(group.getHardCpuLimit().get()) <= 0, "Soft CPU limit cannot be greater than hard CPU limit");
            }
        }

        ImmutableList.Builder<ResourceGroupSelector> selectors = ImmutableList.builder();
        for (SelectorSpec spec : managerSpec.getSelectors()) {
            selectors.add(new StaticSelector(spec.getUserRegex(), spec.getSourceRegex(), spec.getGroup()));
        }
        this.selectors = selectors.build();

        memoryPoolManager.addChangeListener(new MemoryPoolId("general"), poolInfo -> {
            synchronized (generalPoolMemoryFraction) {
                for (Map.Entry<ResourceGroup, Double> entry : generalPoolMemoryFraction.entrySet()) {
                    double bytes = poolInfo.getMaxBytes() * entry.getValue();
                    entry.getKey().setSoftMemoryLimit(new DataSize(bytes, BYTE));
                }
                generalPoolBytes = poolInfo.getMaxBytes();
            }
        });
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext context)
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
        if (match.getSoftCpuLimit().isPresent() || match.getHardCpuLimit().isPresent()) {
            checkState(cpuQuotaPeriodMillis.isPresent());
            Duration limit;
            if (match.getHardCpuLimit().isPresent()) {
                limit = match.getHardCpuLimit().get();
            }
            else {
                limit = match.getSoftCpuLimit().get();
            }
            long rate = (long) Math.min(1000.0 * limit.toMillis() / (double) cpuQuotaPeriodMillis.get().toMillis(), Long.MAX_VALUE);
            rate = Math.max(1, rate);
            group.setCpuQuotaGenerationMillisPerSecond(rate);
        }
        if (match.getSoftCpuLimit().isPresent()) {
            group.setSoftCpuLimit(match.getSoftCpuLimit().get());
        }
        if (match.getHardCpuLimit().isPresent()) {
            group.setHardCpuLimit(match.getHardCpuLimit().get());
        }
    }

    @Override
    public List<ResourceGroupSelector> getSelectors()
    {
        return selectors;
    }
}
