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
package com.facebook.presto.resourceGroups.db;

import com.facebook.presto.resourceGroups.reloading.ManagerSpecProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class DbResourceGroupsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
        binder.bind(ResourceGroupsDao.class).toProvider(MysqlDaoProvider.class).in(Scopes.SINGLETON);
        binder.bind(ManagerSpecProvider.class).to(DbManagerSpecProvider.class).in(Scopes.SINGLETON);
    }
}
