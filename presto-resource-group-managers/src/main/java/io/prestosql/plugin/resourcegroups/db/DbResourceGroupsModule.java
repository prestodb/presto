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
package io.prestosql.plugin.resourcegroups.db;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DbResourceGroupsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
        binder.bind(ResourceGroupsDao.class).toProvider(MysqlDaoProvider.class).in(Scopes.SINGLETON);
        binder.bind(DbResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        binder.bind(ResourceGroupConfigurationManager.class).to(DbResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DbResourceGroupConfigurationManager.class).withGeneratedName();
    }
}
