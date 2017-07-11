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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestSetSessionTask
{
    private static final String CATALOG_NAME = "foo";
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final MetadataManager metadata;

    public TestSetSessionTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = new MetadataManager(
                new FeaturesConfig(),
                new TypeRegistry(),
                new BlockEncodingManager(new TypeRegistry()),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager);

        metadata.getSessionPropertyManager().addSystemSessionProperty(stringSessionProperty(
                CATALOG_NAME,
                "test property",
                null,
                false));

        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);
        metadata.getSessionPropertyManager().addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), ImmutableList.of(stringSessionProperty(
                "bar",
                "test property",
                null,
                false)));
        catalogManager.registerCatalog(bogusTestingCatalog);
    }

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stage-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        testSetSession(new StringLiteral("baz"), "baz");
        testSetSession(new FunctionCall(QualifiedName.of("concat"), ImmutableList.of(
                new StringLiteral("ban"),
                new StringLiteral("ana"))), "banana");
    }

    @Test
    public void testSetSessionWithParameters()
            throws Exception
    {
        List<Expression> expressionList = new ArrayList<>();
        expressionList.add(new StringLiteral("ban"));
        expressionList.add(new Parameter(0));
        testSetSessionWithParameters(new FunctionCall(QualifiedName.of("concat"), expressionList), "banana", ImmutableList.of(new StringLiteral("ana")));
    }

    private void testSetSession(Expression expression, String expectedValue)
            throws Exception
    {
        testSetSessionWithParameters(expression, expectedValue, emptyList());
    }

    private void testSetSessionWithParameters(Expression expression, String expectedValue, List<Expression> parameters)
            throws Exception
    {
        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("query"), "set foo.bar = 'baz'", TEST_SESSION, URI.create("fake://uri"), false, transactionManager, accessControl, executor, metadata);
        getFutureValue(new SetSessionTask().execute(new SetSession(QualifiedName.of(CATALOG_NAME, "bar"), expression), transactionManager, metadata, accessControl, stateMachine, parameters));

        Map<String, String> sessionProperties = stateMachine.getSetSessionProperties();
        assertEquals(sessionProperties, ImmutableMap.of("foo.bar", expectedValue));
    }
}
