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
import java.util.Map;
import java.util.Properties;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
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

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        directory = createTempDirectory("test-optimizers").toFile();
        directory.deleteOnExit();

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        PluginNodeManager pluginNodeManager = new PluginNodeManager(nodeManager);
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
        manager.loadExpressionOptimizerFactories();

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
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories());
    }

    @Test
    public void testNoFactoryName()
            throws Exception
    {
        createPropertiesFile("foo.properties", ImmutableMap.of());

        manager.addExpressionOptimizerFactory(getExpressionOptimizerFactory("foo"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories());
    }

    @Test
    public void testNoFactoryRegistered()
            throws Exception
    {
        createPropertiesFile("foo.properties", ImmutableMap.of("expression-manager-factory.name", "foo"));
        assertThrows(IllegalArgumentException.class, () -> manager.loadExpressionOptimizerFactories());
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
        File newProperties = new File(directory, fileName);
        newProperties.deleteOnExit();
        Properties properties = new Properties();
        properties.putAll(propertiesMap);
        properties.store(newOutputStream(newProperties.toPath()), null);
    }

    public ExpressionOptimizerFactory getExpressionOptimizerFactory(String name)
    {
        return new ExpressionOptimizerFactory() {
            @Override
            public ExpressionOptimizer createOptimizer(Map<String, String> config, ExpressionOptimizerContext context)
            {
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
}
