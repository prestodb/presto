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
package com.facebook.presto.plugin.turbonium.config.db;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class ConfigDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder ignored)
    {
        install(installModuleIf(
                TurboniumDbConfig.class,
                config -> "mysql".equals(config.getConfigDbType()),
                binder -> binder.bind(TurboniumConfigDao.class).toProvider(MySqlDaoProvider.class).in(Scopes.SINGLETON)));
        install(installModuleIf(
                TurboniumDbConfig.class,
                config -> "h2".equals(config.getConfigDbType()),
                binder -> binder.bind(TurboniumConfigDao.class).toProvider(H2DaoProvider.class).in(Scopes.SINGLETON)));
    }
}
