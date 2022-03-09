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

package com.facebook.presto.hive.functions;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import static java.util.Objects.requireNonNull;

public class HiveFunctionModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final ClassLoader classLoader;
    private final TypeManager typeManager;

    public HiveFunctionModule(String catalogName, ClassLoader classLoader, TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(new TypeLiteral<String>() {}).annotatedWith(ForHiveFunction.class).toInstance(catalogName);
        binder.bind(HiveFunctionRegistry.class).to(StaticHiveFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(FunctionNamespaceManager.class).to(HiveFunctionNamespaceManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForHiveFunction
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }
}
