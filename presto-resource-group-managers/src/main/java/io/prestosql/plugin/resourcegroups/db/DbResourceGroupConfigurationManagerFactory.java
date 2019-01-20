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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.resourcegroups.VariableMap;
import io.prestosql.spi.memory.ClusterMemoryPoolManager;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManager;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerContext;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

public class DbResourceGroupConfigurationManagerFactory
        implements ResourceGroupConfigurationManagerFactory
{
    @Override
    public String getName()
    {
        return "db";
    }

    @Override
    public ResourceGroupConfigurationManager<VariableMap> create(Map<String, String> config, ResourceGroupConfigurationManagerContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new DbResourceGroupsModule(),
                    new PrefixObjectNameGeneratorModule(),
                    binder -> binder.bind(String.class).annotatedWith(ForEnvironment.class).toInstance(context.getEnvironment()),
                    binder -> binder.bind(ClusterMemoryPoolManager.class).toInstance(context.getMemoryPoolManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(DbResourceGroupConfigurationManager.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
