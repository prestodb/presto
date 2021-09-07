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
package com.facebook.presto.execution.scheduler.nodeselection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NodeSelectionConfigurationSpec
{
    private Set<String> defaultApplicablePools;
    private List<NodeSelectionCriteriaSpec> nodeSelectionCriteriaSpecs;

    @JsonCreator
    public NodeSelectionConfigurationSpec(@JsonProperty("defaultApplicablePools") Set<String> defaultApplicablePools, @JsonProperty("nodeSelectionCriteria") List<NodeSelectionCriteriaSpec> nodeSelectionCriteriaSpecs)
    {
        this.defaultApplicablePools = requireNonNull(defaultApplicablePools, "defaultApplicablePools is null");
        this.nodeSelectionCriteriaSpecs = requireNonNull(nodeSelectionCriteriaSpecs, "nodeSelectionCriteriaSpecs is null");
    }

    @JsonProperty
    public List<NodeSelectionCriteriaSpec> getNodeSelectionCriteriaSpecs()
    {
        return nodeSelectionCriteriaSpecs;
    }

    @JsonProperty
    public Set<String> getDefaultApplicablePools()
    {
        return defaultApplicablePools;
    }

    public static class NodeSelectionCriteriaSpec
    {
        private final Optional<Set<String>> clientTags;
        private final Set<String> pools;

        @JsonCreator
        public NodeSelectionCriteriaSpec(@JsonProperty("clientTags") Optional<Set<String>> clientTags, @JsonProperty("pools") Set<String> pools)
        {
            this.clientTags = requireNonNull(clientTags, "clientTags is null");
            this.pools = ImmutableSet.copyOf(requireNonNull(pools, "pools is null"));
        }

        @JsonProperty
        public Optional<Set<String>> getClientTags()
        {
            return clientTags;
        }

        @JsonProperty
        public Set<String> getPools()
        {
            return pools;
        }
    }
}
