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
package com.facebook.presto.sql.expressions;

import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.connector.ConnectorAwareNodeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterService;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterServiceFactory;
import com.facebook.presto.sql.planner.JavaEvalRowExpressionInterpreterService;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class ExpressionManager
{
    private static final File EXPRESSION_MANAGER_CONFIGURATION = new File("etc/expression-manager.properties");
    private static final String EXPRESSION_MANAGER_FACTORY_NAME = "expression-manager-factory.name";

    private final Map<String, RowExpressionInterpreterServiceFactory> rowExpressionInterpreterFactories = new ConcurrentHashMap<>();
    private final AtomicReference<RowExpressionInterpreterService> rowExpressionInterpreter = new AtomicReference<>();
    private final ConnectorAwareNodeManager nodeManager;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final RowExpressionSerde rowExpressionSerde;

    @Inject
    public ExpressionManager(InternalNodeManager nodeManager, FunctionAndTypeManager functionAndTypeManager, NodeInfo nodeInfo, RowExpressionSerde rowExpressionSerde)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        this.nodeManager = new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), new ConnectorId("dummy")); // TODO
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    public void loadExpressions()
            throws IOException
    {
        if (EXPRESSION_MANAGER_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(EXPRESSION_MANAGER_CONFIGURATION));
            String factoryName = properties.remove(EXPRESSION_MANAGER_FACTORY_NAME);
            checkArgument(!isNullOrEmpty(factoryName), "%s does not contain %s", EXPRESSION_MANAGER_CONFIGURATION, EXPRESSION_MANAGER_FACTORY_NAME);
            checkArgument(
                    rowExpressionInterpreter.compareAndSet(null, rowExpressionInterpreterFactories.get(factoryName).createInterpreter(properties, nodeManager, rowExpressionSerde)),
                    "ExpressionManager is already loaded");
        }
        else {
            checkArgument(
                    rowExpressionInterpreter.compareAndSet(null, new JavaEvalRowExpressionInterpreterService(functionAndTypeManager)),
                    "ExpressionManager is already loaded");
        }
    }

    public void addBatchRowExpressionInterpreterProvider(RowExpressionInterpreterServiceFactory batchRowExpressionInterpreterProvider)
    {
        String name = batchRowExpressionInterpreterProvider.getName();
        checkArgument(
                rowExpressionInterpreterFactories.putIfAbsent(name, batchRowExpressionInterpreterProvider) == null,
                "BatchRowExpressionInterpreterProviderFactory %s is already registered", name);
    }

    public RowExpressionInterpreterService getRowExpressionInterpreter()
    {
        return rowExpressionInterpreter.get();
    }
}
