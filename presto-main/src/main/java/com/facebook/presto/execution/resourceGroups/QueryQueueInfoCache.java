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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class QueryQueueInfoCache
{
    private final AtomicReference<Map<ResourceGroupId, List<QueryEntry>>> queueInfo = new AtomicReference(new HashMap<>());

    public static class QueryEntry
    {
        private final ResourceGroupId leafGroupId;
        private final QueryId queryId;
        private final int approximatePosition;
        private final boolean isQueued;

        public QueryEntry(ResourceGroupId leafGroupId, QueryId queryId, int approximatePosition, boolean isQueued)
        {
            this.leafGroupId = leafGroupId;
            this.queryId = queryId;
            this.approximatePosition = approximatePosition;
            this.isQueued = isQueued;
        }

        public ResourceGroupId getLeafGroupId()
        {
            return leafGroupId;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public int getApproximatePosition()
        {
            return approximatePosition;
        }

        public boolean getIsQueued()
        {
            return isQueued;
        }
    }

    public Map<ResourceGroupId, List<QueryEntry>> getInfo()
    {
        return queueInfo.get();
    }

    public void refresh(List<InternalResourceGroup.RootInternalResourceGroup> rootGroups)
    {
        Map<ResourceGroupId, List<QueryEntry>> info = new HashMap<>();
        for (InternalResourceGroup.RootInternalResourceGroup rootGroup : rootGroups) {
            info.put(rootGroup.getId(), refreshRootGroup(rootGroup));
        }
        queueInfo.set(info);
    }

    private List<QueryEntry> refreshRootGroup(InternalResourceGroup.RootInternalResourceGroup root)
    {
        List<QueryEntry> queryEntries = new ArrayList<>();
        LinkedList<InternalResourceGroup> stack = new LinkedList<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            InternalResourceGroup group = stack.poll();
            List<InternalResourceGroup> subGroups = group.getActiveSubGroups();
            if (!subGroups.isEmpty()) {
                stack.addAll(subGroups);
            }
            for (QueryId queryId : group.getRunningQueryIds()) {
                queryEntries.add(new QueryEntry(group.getId(), queryId, 0, false));
            }
            int position = 1;
            for (QueryId queryId : group.getQueuedQueryIds()) {
                queryEntries.add(new QueryEntry(group.getId(), queryId, position++, true));
            }
        }
        return queryEntries;
    }
}
