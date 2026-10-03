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
package com.facebook.presto.ducklake;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.ducklake.catalog.postgresql.PostgreSqlCatalogModule;
import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.presto.ducklake.CatalogType.POSTGRESQL;

public class DuckLakeCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindCatalogModule(POSTGRESQL, new PostgreSqlCatalogModule());
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        install(installModuleIf(
                DuckLakeConfig.class,
                config -> config.getCatalogType() == catalogType,
                module));
    }
}
