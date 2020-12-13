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
package com.facebook.presto.spi;

import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.List;
import java.util.OptionalLong;

public interface ConnectorSplit
{
    /**
     * Indicate the node affinity of a Split
     * 1. HARD_AFFINITY: Split is NOT remotely accessible and has to be on specific nodes
     * 2. SOFT_AFFINITY: Connector split provides a list of preferred nodes for engine to pick from but not mandatory.
     * 3. NO_PREFERENCE: Split is remotely accessible and can be on any nodes
     */
    NodeSelectionStrategy getNodeSelectionStrategy();

    /**
     * Provide a list of preferred nodes for scheduler to pick.
     * 1. The scheduler will respect the preference if the strategy is HARD_AFFINITY.
     * 2. Otherwise, the scheduler will prioritize the provided nodes if the strategy is SOFT_AFFINITY.
     * But there is no guarantee that the scheduler will pick them if the provided nodes are busy.
     * 3. Empty list indicates no preference.
     */
    List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates);

    Object getInfo();

    default Object getSplitIdentifier()
    {
        return this;
    }

    default OptionalLong getSplitSizeInBytes()
    {
        return OptionalLong.empty();
    }
}
