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

import java.util.List;

/**
 * Represents algorithm to select nodes from a set of candidates
 * based on certain user provided hint.
 */
public interface NodeSelection
{
    /**
     * Selects the elements from candidate nodes and returns based on
     * hint passed.
     *
     * @param candidates Nodes to select from
     * @param hint Hint to the selection algorithm
     * @return List of selected nodes.
     */
    List<InternalNode> select(List<InternalNode> candidates, NodeSelectionHint hint);
}
