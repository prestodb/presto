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
package com.facebook.presto.execution.resourceGroups.db;

import com.facebook.presto.resourceGroups.db.DbResourceGroupConfig;
import com.facebook.presto.resourceGroups.db.DbResourceGroupConfigurationManager;
import com.facebook.presto.resourceGroups.db.H2DaoProvider;
import com.facebook.presto.resourceGroups.db.ResourceGroupsDao;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class H2ResourceGroupsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
        binder.bind(ResourceGroupsDao.class).toProvider(H2DaoProvider.class).in(Scopes.SINGLETON);
        binder.bind(DbResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        binder.bind(ResourceGroupConfigurationManager.class).to(DbResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
    }
}
