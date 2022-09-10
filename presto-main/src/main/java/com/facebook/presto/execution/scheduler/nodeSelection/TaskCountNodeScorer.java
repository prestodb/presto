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

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.metadata.InternalNode;
import com.google.inject.Inject;

public class TaskCountNodeScorer implements NodeScorer
{
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public TaskCountNodeScorer(NodeTaskMap nodeTaskMap) {
        this.nodeTaskMap = nodeTaskMap;
    }

    @Override
    public long score(InternalNode node)
    {
        return nodeTaskMap.getPartitionedSplitsOnNode(node).getCount();
    }
}
