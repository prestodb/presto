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
package com.facebook.presto.execution;

import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSetSessionTask
{
    private static final String CATALOG_NAME = "foo";
    private static final String MUST_BE_POSITIVE = "property must be positive";
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final MetadataManager metadata;

    public TestSetSessionTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());

        metadata.getSessionPropertyManager().addSystemSessionProperty(stringProperty(
                CATALOG_NAME,
                "test property",
                null,
                false));

        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);

        List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
                stringProperty(
                        "bar",
                        "test property",
                        null,
                        false),
                new PropertyMetadata<>(
                        "positive_property",
                        "property that should be positive",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> validatePositive(value),
                        value -> value));

        metadata.getSessionPropertyManager().addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), sessionProperties);

        catalogManager.registerCatalog(bogusTestingCatalog);
    }

    private static int validatePositive(Object value)
    {
        int intValue = ((Number) value).intValue();
        if (intValue < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, MUST_BE_POSITIVE);
        }
        return intValue;
    }

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSetSession()
    {
        testSetSession(new StringLiteral("baz"), "baz");
        testSetSession(new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                new StringLiteral("ban"),
                new StringLiteral("ana"))), "banana");
    }

    @Test
    public void testSetSessionWithValidation()
    {
        testSetSessionWithValidation(new LongLiteral("0"), "0");
        testSetSessionWithValidation(new LongLiteral("2"), "2");

        try {
            testSetSessionWithValidation(new LongLiteral("-1"), "-1");
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), MUST_BE_POSITIVE);
        }
    }

    @Test
    public void testSetSessionWithParameters()
    {
        List<Expression> expressionList = new ArrayList<>();
        expressionList.add(new StringLiteral("ban"));
        expressionList.add(new Parameter(0));
        testSetSessionWithParameters("bar", new FunctionCall(QualifiedName.of("concat"), expressionList), "banana", ImmutableList.of(new StringLiteral("ana")));
    }

    private void testSetSession(Expression expression, String expectedValue)
    {
        testSetSessionWithParameters("bar", expression, expectedValue, emptyList());
    }

    private void testSetSessionWithValidation(Expression expression, String expectedValue)
    {
        testSetSessionWithParameters("positive_property", expression, expectedValue, emptyList());
    }

    private void testSetSessionWithParameters(String property, Expression expression, String expectedValue, List<Expression> parameters)
    {
        QualifiedName qualifiedPropName = QualifiedName.of(CATALOG_NAME, property);
        String sqlString = format("set %s = 'old_value'", qualifiedPropName);
        QueryStateMachine stateMachine = createQueryStateMachine(sqlString, TEST_SESSION, false, transactionManager, executor, metadata);
        getFutureValue(new SetSessionTask().execute(new SetSession(qualifiedPropName, expression), transactionManager, metadata, accessControl, stateMachine, parameters));

        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties, ImmutableMap.of(qualifiedPropName.toString(), expectedValue));
    }
}
