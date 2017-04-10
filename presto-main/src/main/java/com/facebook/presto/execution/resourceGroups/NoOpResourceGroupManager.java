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

import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.sql.tree.Statement;

import java.util.concurrent.Executor;

/**
 * Used on workers.
 */
public final class NoOpResourceGroupManager
        implements ResourceGroupManager
{
    @Override
    public void submit(Statement statement, QueryExecution queryExecution, Executor executor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResourceGroupInfo getResourceGroupInfo(ResourceGroupId id)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory)
    {
        // no-op
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        // no-op
    }
}
