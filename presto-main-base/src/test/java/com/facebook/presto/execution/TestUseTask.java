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

import com.facebook.presto.Session;
import com.facebook.presto.connector.MockConnectorFactory;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.DenyAllAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Use;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.connector.MockConnectorFactory.builder;
import static com.facebook.presto.execution.TaskTestUtils.createQueryStateMachine;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestUseTask
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    private CatalogManager catalogManager;
    private TransactionManager transactionManager;
    private MetadataManager metadata;

    @BeforeClass
    public void setUp()
    {
        catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testUse()
    {
        Use use = new Use(Optional.of(new Identifier("test_catalog")), new Identifier("test_schema"));
        String sqlString = "USE test_catalog.test_schema";
        AccessControl accessControl = new AllowAllAccessControl();
        executeUse(use, sqlString, TEST_SESSION, accessControl);
    }

    @Test(
            expectedExceptions = SemanticException.class,
            expectedExceptionsMessageRegExp = "Catalog must be specified when session catalog is not set")
    public void testUseNoCatalog()
    {
        Use use = new Use(Optional.empty(), new Identifier("test_schema"));
        String sqlString = "USE test_schema";
        Session session = testSessionBuilder()
                .setCatalog(null)
                .setSchema(null)
                .build();
        AccessControl accessControl = new AllowAllAccessControl();
        executeUse(use, sqlString, session, accessControl);
    }

    @Test(
            expectedExceptions = SemanticException.class,
            expectedExceptionsMessageRegExp = "Catalog does not exist: invalid_catalog")
    public void testUseInvalidCatalog()
    {
        Use use = new Use(Optional.of(new Identifier("invalid_catalog")), new Identifier("test_schema"));
        String sqlString = "USE invalid_catalog.test_schema";
        AccessControl accessControl = new AllowAllAccessControl();
        executeUse(use, sqlString, TEST_SESSION, accessControl);
    }

    @Test(
            expectedExceptions = SemanticException.class,
            expectedExceptionsMessageRegExp = "Schema does not exist: test_catalog.invalid_schema")
    public void testUseInvalidSchema()
    {
        Use use = new Use(Optional.of(new Identifier("test_catalog")), new Identifier("invalid_schema"));
        String sqlString = "USE test_catalog.invalid_schema";
        Session session = testSessionBuilder()
                .setSchema("invalid_schema")
                .build();
        AccessControl accessControl = new AllowAllAccessControl();
        executeUse(use, sqlString, session, accessControl);
    }

    @Test(
            expectedExceptions = AccessDeniedException.class,
            expectedExceptionsMessageRegExp = "Access Denied: Cannot access catalog test_catalog")
    public void testUseAccessDenied()
    {
        Use use = new Use(Optional.of(new Identifier("test_catalog")), new Identifier("test_schema"));
        String sqlString = "USE test_catalog.test_schema";
        Session session = testSessionBuilder()
                .setIdentity(new Identity("user", Optional.empty()))
                .build();
        AccessControl accessControl = new DenyAllAccessControl();
        executeUse(use, sqlString, session, accessControl);
    }

    private void executeUse(Use use, String sqlString, Session session, AccessControl accessControl)
    {
        MockConnectorFactory mockConnectorFactory = builder()
                .withListSchemaNames(connectorSession -> ImmutableList.of("test_schema"))
                .build();
        catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        metadata = createTestMetadataManager(transactionManager);

        Connector testConnector = mockConnectorFactory.create("test", ImmutableMap.of(), new TestingConnectorContext());
        String catalogName = "test_catalog";
        ConnectorId connectorId = new ConnectorId(catalogName);
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                testConnector,
                createInformationSchemaConnectorId(connectorId),
                testConnector,
                createSystemTablesConnectorId(connectorId),
                testConnector));

        QueryStateMachine stateMachine = createQueryStateMachine(sqlString, session, false, transactionManager, executor, metadata);
        UseTask useTask = new UseTask();
        useTask.execute(use, transactionManager, metadata, accessControl, stateMachine, emptyList(), sqlString);
    }
}
