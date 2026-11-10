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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerContext;
import com.facebook.presto.spi.sql.planner.ExpressionOptimizerFactory;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.AuthClientConfigs.defaultAuthClientConfigs;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.newOutputStream;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

@Test(singleThreaded = true)
public class TestExpressionOptimizerManager
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private File directory;
    private ExpressionOptimizerManager manager;
    private PluginNodeManager pluginNodeManager;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        directory = createTempDirectory("test-optimizers").toFile();
        directory.deleteOnExit();

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        pluginNodeManager = new PluginNodeManager(nodeManager);
        manager = new ExpressionOptimizerManager(
                pluginNodeManager,
                METADATA.getFunctionAndTypeManager(),
                new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)),
                directory);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        deleteRecursively(directory.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testBasicIntegration()
            throws Exception
    {
        createPropertiesFile("foo.properties", ImmutableMap.of("expression-manager-factory.name", "foo"));
        createPropertiesFile("bar.properties", ImmutableMap.of("expression-manager-factory.name", "bar"));

        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("bar"));
        manager.loadExpressionOptimizerFactories(defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier()));

        assertOptimizedExpression("1+1", "2", ImmutableMap.of());
        assertOptimizedExpression("1+1", "2", ImmutableMap.of("expression_optimizer_name", "default"));

        // Override the default optimizer based on session property
        assertOptimizedExpression("1+1", "'foo'", ImmutableMap.of("expression_optimizer_name", "foo"));
        assertOptimizedExpression("1+1", "'bar'", ImmutableMap.of("expression_optimizer_name", "bar"));
    }

    @Test
    public void testNoNewOptimizerNameCalledDefault()
            throws Exception
    {
        createPropertiesFile("default.properties", ImmutableMap.of("expression-manager-factory.name", "default"));

        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("default"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories(defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testNoFactoryName()
            throws Exception
    {
        createPropertiesFile("foo.properties", ImmutableMap.of());

        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories(defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testNoFactoryRegistered()
            throws Exception
    {
        createPropertiesFile("foo.properties", ImmutableMap.of("expression-manager-factory.name", "foo"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories(defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testLoadFromMap()
    {
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("bar"));

        manager.loadExpressionOptimizers(
                ImmutableMap.of(
                        "ai-function-rewrite", ImmutableMap.of("expression-manager-factory.name", "foo"),
                        "bar", ImmutableMap.of("expression-manager-factory.name", "bar")),
                defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier()));

        // Resolving by name is the exact call RewriteRowExpressions makes during planning.
        assertEquals(
                manager.getExpressionOptimizer("ai-function-rewrite").optimize(expression("1+1"), OPTIMIZED, testSessionBuilder().build().toConnectorSession()),
                expression("'foo'"));
        assertEquals(
                manager.getExpressionOptimizer("bar").optimize(expression("1+1"), OPTIMIZED, testSessionBuilder().build().toConnectorSession()),
                expression("'bar'"));

        assertOptimizedExpression("1+1", "2", ImmutableMap.of());
        assertOptimizedExpression("1+1", "'foo'", ImmutableMap.of("expression_optimizer_name", "ai-function-rewrite"));
    }

    @Test
    public void testLoadFromMapPassesConfigThroughWithoutFactoryName()
    {
        Map<String, String> received = new HashMap<>();
        manager.addExpressionOptimizerFactory(getConfigCapturingFactory("capturing", received));

        manager.loadExpressionOptimizers(
                ImmutableMap.of("ai-function-rewrite", ImmutableMap.of(
                        "expression-manager-factory.name", "capturing",
                        "allowed-catalogs", "meta")),
                defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier()));

        assertEquals(received, ImmutableMap.of("allowed-catalogs", "meta"));
    }

    @Test
    public void testLoadFromMapDoesNotMutateCallerMap()
    {
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));

        Map<String, Map<String, String>> properties = ImmutableMap.of("ai-function-rewrite", ImmutableMap.of("expression-manager-factory.name", "foo"));
        manager.loadExpressionOptimizers(properties, defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier()));

        assertEquals(properties.get("ai-function-rewrite"), ImmutableMap.of("expression-manager-factory.name", "foo"));
    }

    @Test
    public void testLoadFromMapNoNewOptimizerNameCalledDefault()
    {
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("default"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizers(
                ImmutableMap.of("default", ImmutableMap.of("expression-manager-factory.name", "default")),
                defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testLoadFromMapNoFactoryName()
    {
        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizers(
                ImmutableMap.of("ai-function-rewrite", ImmutableMap.of()),
                defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testLoadFromMapNoFactoryRegistered()
    {
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizers(
                ImmutableMap.of("ai-function-rewrite", ImmutableMap.of("expression-manager-factory.name", "ai-function-rewrite")),
                defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier())));
    }

    @Test
    public void testLoadFromEmptyMapIsNoOp()
    {
        manager.loadExpressionOptimizers(ImmutableMap.of(), defaultAuthClientConfigs(pluginNodeManager.getCurrentNode().getNodeIdentifier()));
        assertOptimizedExpression("1+1", "2", ImmutableMap.of());
    }

    private void assertOptimizedExpression(String originalExpression, String optimizedExpression, Map<String, String> systemProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder();
        systemProperties.forEach(sessionBuilder::setSystemProperty);
        Session session = sessionBuilder.build();
        assertEquals(manager.getExpressionOptimizer(session.toConnectorSession()).optimize(expression(originalExpression), OPTIMIZED, session.toConnectorSession()),
                expression(optimizedExpression));
    }

    private static RowExpression expression(String expression)
    {
        return TRANSLATOR.translate(expression, ImmutableMap.of());
    }

    private void createPropertiesFile(String fileName, Map<String, String> propertiesMap)
            throws IOException
    {
        File newProperties = directory.toPath().resolve(fileName).toFile();
        newProperties.deleteOnExit();
        Properties properties = new Properties();
        properties.putAll(propertiesMap);
        properties.store(newOutputStream(newProperties.toPath()), null);
    }

    public ExpressionOptimizerFactory getExpressionOptimizerFactory(String name)
    {
        return new ExpressionOptimizerFactory()
        {
            @Override
            public ExpressionOptimizer createOptimizer(Map<String, String> config, ExpressionOptimizerContext context)
            {
                // verify if AuthClientConfigs properly propagated into ExpressionOptimizerContext
                assertEquals(
                        context.getAuthClientConfigs().getNodeId(),
                        pluginNodeManager.getCurrentNode().getNodeIdentifier(),
                        "AuthClientConfigs.nodeId should match current plugin node identifier");
                return (expression, level, session, variableResolver) -> constant(
                        Slices.utf8Slice(name),
                        METADATA.getType(TypeSignature.parseTypeSignature(format("varchar(%s)", name.length()))));
            }

            @Override
            public String getName()
            {
                return name;
            }
        };
    }

    private ExpressionOptimizerFactory getConfigCapturingFactory(String name, Map<String, String> received)
    {
        return new ExpressionOptimizerFactory()
        {
            @Override
            public ExpressionOptimizer createOptimizer(Map<String, String> config, ExpressionOptimizerContext context)
            {
                received.putAll(config);
                return (expression, level, session, variableResolver) -> expression;
            }

            @Override
            public String getName()
            {
                return name;
            }
        };
    }
}
