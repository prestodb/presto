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
package io.prestosql.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BucketReassigner
{
    private final Map<String, Integer> nodeBucketCounts = new HashMap<>();
    private final List<String> activeNodes;
    private final List<BucketNode> bucketNodes;
    private boolean initialized;

    public BucketReassigner(Set<String> activeNodes, List<BucketNode> bucketNodes)
    {
        checkArgument(!activeNodes.isEmpty(), "activeNodes must not be empty");
        this.activeNodes = ImmutableList.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
        this.bucketNodes = requireNonNull(bucketNodes, "bucketNodes is null");
    }

    // NOTE: This method is not thread safe
    public String getNextReassignmentDestination()
    {
        if (!initialized) {
            for (String node : activeNodes) {
                nodeBucketCounts.put(node, 0);
            }
            for (BucketNode bucketNode : bucketNodes) {
                nodeBucketCounts.computeIfPresent(bucketNode.getNodeIdentifier(), (node, bucketCount) -> bucketCount + 1);
            }
            initialized = true;
        }

        String assignedNode;
        if (activeNodes.size() > 1) {
            assignedNode = randomTwoChoices();
        }
        else {
            assignedNode = activeNodes.get(0);
        }

        nodeBucketCounts.compute(assignedNode, (node, count) -> count + 1);
        return assignedNode;
    }

    private String randomTwoChoices()
    {
        // Purely random choices can overload unlucky node while selecting the least loaded one based on stale
        // local information can overload the previous idle node. Here we randomly pick 2 nodes and select the
        // less loaded one. This prevents those issues and renders good enough load balance.
        int randomPosition = ThreadLocalRandom.current().nextInt(activeNodes.size());
        int randomOffset = ThreadLocalRandom.current().nextInt(1, activeNodes.size());
        String candidate1 = activeNodes.get(randomPosition);
        String candidate2 = activeNodes.get((randomPosition + randomOffset) % activeNodes.size());

        return (nodeBucketCounts.get(candidate1) <= nodeBucketCounts.get(candidate2)) ? candidate1 : candidate2;
    }
}
