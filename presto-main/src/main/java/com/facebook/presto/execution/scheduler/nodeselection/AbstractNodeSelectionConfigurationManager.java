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

import com.facebook.presto.execution.scheduler.nodeselection.NodeSelectionConfigurationSpec.NodeSelectionCriteriaSpec;

import java.util.Optional;
import java.util.Set;

public abstract class AbstractNodeSelectionConfigurationManager
        implements NodeSelectionConfigurationManager
{
    public Optional<Set<String>> getApplicablePools(NodeSelectionCriteria nodeSelectionCriteria)
    {
        //There could be different ways to load the spec, so abstract it out to inject custom implementation for loading the spec.
        NodeSelectionConfigurationSpec nodeSelectionConfigurationSpec = loadNodeSelectionConfigurationSpec();

        return Optional.of(nodeSelectionConfigurationSpec.getNodeSelectionCriteriaSpecs().stream()
                .map(criteriaSpec -> match(criteriaSpec, nodeSelectionCriteria))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst().orElse(nodeSelectionConfigurationSpec.getDefaultApplicablePools()));
    }

    private Optional<Set<String>> match(NodeSelectionCriteriaSpec criteriaSpec, NodeSelectionCriteria nodeSelectionCriteria)
    {
        if (criteriaSpec.getClientTags().isPresent() && criteriaSpec.getClientTags().get().containsAll(nodeSelectionCriteria.getClientTags())) {
            return Optional.of(criteriaSpec.getPools());
        }
        return Optional.empty();
    }

    protected abstract NodeSelectionConfigurationSpec loadNodeSelectionConfigurationSpec();
}
