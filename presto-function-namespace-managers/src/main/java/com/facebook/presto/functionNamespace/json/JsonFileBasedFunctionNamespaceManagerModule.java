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
package com.facebook.presto.functionNamespace.json;

import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionLanguageConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class JsonFileBasedFunctionNamespaceManagerModule
        implements Module
{
    private final String catalogName;

    public JsonFileBasedFunctionNamespaceManagerModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<String>() {}).annotatedWith(ServingCatalog.class).toInstance(catalogName);

        configBinder(binder).bindConfig(JsonFileBasedFunctionNamespaceManagerConfig.class);
        configBinder(binder).bindConfig(SqlInvokedFunctionNamespaceManagerConfig.class);
        configBinder(binder).bindConfig(SqlFunctionLanguageConfig.class);
        binder.bind(FunctionDefinitionProvider.class).to(JsonFileBasedFunctionDefinitionProvider.class).in(SINGLETON);
        binder.bind(JsonFileBasedFunctionNamespaceManager.class).in(SINGLETON);
    }
}
