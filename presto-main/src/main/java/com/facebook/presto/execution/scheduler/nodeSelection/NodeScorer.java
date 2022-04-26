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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.metadata.InternalNode;
import it.unimi.dsi.fastutil.longs.LongImmutableList;

import java.util.List;

/**
 * The interface represents scorer that is used to
 * assign score / weights to node based on various
 * criteria.
 */
public interface NodeScorer
{
    /**
     * Computes score for the given node.
     *
     * @param node InternalNode instance to score
     * @return numeric score for the node
     */
    long score(InternalNode node);

    /**
     * Computes the score for the list of nodes.
     *
     * @param nodeList List of InternalNode instances to score
     * @return List of scores. The score for a particular node
     * is available at the corresponding index on the output
     * list.
     */
    default LongImmutableList score(List<InternalNode> nodeList)
    {
        return LongImmutableList.toListWithExpectedSize(nodeList.stream().mapToLong(this::score), nodeList.size());
    }
}
