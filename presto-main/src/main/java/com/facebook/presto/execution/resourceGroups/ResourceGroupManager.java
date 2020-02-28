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

import com.facebook.presto.execution.ManagedQueryExecution;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.sql.tree.Statement;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Classes implementing this interface must be thread safe. That is, all the methods listed below
 * may be called concurrently from any thread.
 */
@ThreadSafe
public interface ResourceGroupManager<C>
{
    void submit(Statement statement, ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor);

    SelectionContext<C> selectGroup(SelectionCriteria criteria);

    ResourceGroupInfo getResourceGroupInfo(ResourceGroupId id, boolean includeQueryInfo, boolean summarizeSubgroups, boolean includeStaticSubgroupsOnly);

    List<ResourceGroupInfo> getPathToRoot(ResourceGroupId id);

    void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory);

    void loadConfigurationManager()
            throws Exception;
}
