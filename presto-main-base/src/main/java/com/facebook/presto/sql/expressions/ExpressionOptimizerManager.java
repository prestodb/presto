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

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerContext;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.SystemSessionProperties.getExpressionOptimizerName;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.util.Objects.requireNonNull;

public class ExpressionOptimizerManager
        implements ExpressionOptimizerProvider
{
    public static final String DEFAULT_EXPRESSION_OPTIMIZER_NAME = "default";
    private static final File EXPRESSION_MANAGER_CONFIGURATION_DIRECTORY = new File("etc/expression-manager/");
    private static final String EXPRESSION_MANAGER_FACTORY_NAME = "expression-manager-factory.name";

    private final NodeManager nodeManager;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;
    private final File configurationDirectory;

    private final Map<String, ExpressionOptimizerFactory> expressionOptimizerFactories = new ConcurrentHashMap<>();
    private final Map<String, ExpressionOptimizer> expressionOptimizers = new ConcurrentHashMap<>();

    @Inject
    public ExpressionOptimizerManager(PluginNodeManager nodeManager, FunctionAndTypeManager functionAndTypeManager)
    {
        this(nodeManager, functionAndTypeManager, EXPRESSION_MANAGER_CONFIGURATION_DIRECTORY);
    }

    public ExpressionOptimizerManager(PluginNodeManager nodeManager, FunctionAndTypeManager functionAndTypeManager, File configurationDirectory)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.configurationDirectory = requireNonNull(configurationDirectory, "configurationDirectory is null");
        expressionOptimizers.put(DEFAULT_EXPRESSION_OPTIMIZER_NAME, new RowExpressionOptimizer(functionAndTypeManager));
    }

    public void loadExpressionOptimizerFactories()
    {
        try {
            for (File file : listFiles(configurationDirectory)) {
                if (file.isFile() && file.getName().endsWith(".properties")) {
                    loadExpressionOptimizerFactory(file);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load expression manager configuration", e);
        }
    }

    private void loadExpressionOptimizerFactory(File configurationFile)
            throws IOException
    {
        String optimizerName = getNameWithoutExtension(configurationFile.getName());
        checkArgument(!isNullOrEmpty(optimizerName), "File name is empty, full path: %s", configurationFile.getAbsolutePath());
        checkArgument(!optimizerName.equals(DEFAULT_EXPRESSION_OPTIMIZER_NAME), "Cannot name an expression optimizer instance %s", DEFAULT_EXPRESSION_OPTIMIZER_NAME);

        Map<String, String> properties = new HashMap<>(loadProperties(configurationFile));
        String factoryName = properties.remove(EXPRESSION_MANAGER_FACTORY_NAME);
        checkArgument(!isNullOrEmpty(factoryName), "%s does not contain %s", configurationFile, EXPRESSION_MANAGER_FACTORY_NAME);

        loadExpressionOptimizerFactory(factoryName, optimizerName, properties);
    }

    public void loadExpressionOptimizerFactory(String factoryName, String optimizerName, Map<String, String> properties)
    {
        checkArgument(expressionOptimizerFactories.containsKey(factoryName),
                "ExpressionOptimizerFactory %s is not registered, registered factories: ", factoryName, expressionOptimizerFactories.keySet());

        ExpressionOptimizer optimizer = expressionOptimizerFactories.get(factoryName).createOptimizer(
                properties,
                new ExpressionOptimizerContext(nodeManager, functionAndTypeManager, functionResolution));
        expressionOptimizers.put(optimizerName, optimizer);
    }

    public void addExpressionOptimizerFactory(ExpressionOptimizerFactory expressionOptimizerFactory)
    {
        String name = expressionOptimizerFactory.getName();
        checkArgument(
                expressionOptimizerFactories.putIfAbsent(name, expressionOptimizerFactory) == null,
                "ExpressionOptimizerFactory %s is already registered", name);
    }

    @Override
    public ExpressionOptimizer getExpressionOptimizer(ConnectorSession connectorSession)
    {
        // TODO: Remove this check once we have a more appropriate abstraction for session properties retrieved from plugins
        checkArgument(connectorSession instanceof FullConnectorSession, "connectorSession is not an instance of FullConnectorSession");
        Session session = ((FullConnectorSession) connectorSession).getSession();
        String expressionOptimizerName = getExpressionOptimizerName(session);
        checkArgument(expressionOptimizers.containsKey(expressionOptimizerName), "ExpressionOptimizer '%s' is not registered", expressionOptimizerName);
        return expressionOptimizers.get(expressionOptimizerName);
    }

    private static List<File> listFiles(File directory)
    {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
