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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class FunctionNamespaceLoader
{
    private final Map<String, FunctionNamespaceManagerFactory> functionNamespaceManagerFactories = new ConcurrentHashMap<>();
    private final HandleResolver handleResolver;
    final TransactionManager transactionManager;

    protected final Map<String, FunctionNamespaceManager<? extends SqlFunction>> functionNamespaceManagers = new ConcurrentHashMap<>();

    public FunctionNamespaceLoader(
            TransactionManager transactionManager,
            HandleResolver handleResolver)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.handleResolver = handleResolver;
    }

    public void loadFunctionNamespaceManager(
            String functionNamespaceManagerName,
            String catalogName,
            Map<String, String> properties)
    {
        requireNonNull(functionNamespaceManagerName, "functionNamespaceManagerName is null");
        FunctionNamespaceManagerFactory factory = functionNamespaceManagerFactories.get(functionNamespaceManagerName);
        checkState(factory != null, "No factory for function namespace manager %s", functionNamespaceManagerName);
        FunctionNamespaceManager<?> functionNamespaceManager = factory.create(catalogName, properties);

        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }

    public void addFunctionNamespaceFactory(FunctionNamespaceManagerFactory factory)
    {
        if (functionNamespaceManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
        handleResolver.addFunctionNamespace(factory.getName(), factory.getHandleResolver());
    }

    @VisibleForTesting
    public void addFunctionNamespace(String catalogName, FunctionNamespaceManager functionNamespaceManager)
    {
        transactionManager.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
        if (functionNamespaceManagers.putIfAbsent(catalogName, functionNamespaceManager) != null) {
            throw new IllegalArgumentException(format("Function namespace manager is already registered for catalog [%s]", catalogName));
        }
    }
}
