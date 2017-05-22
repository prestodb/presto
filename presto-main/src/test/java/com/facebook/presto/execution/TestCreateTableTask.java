
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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCreateTableTask
{
    private static final String CATALOG_NAME = "catalog";
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testCreateTableDoesNotThrowAlreadyExistsExceptionWhenIfExists()
            throws Exception
    {
        CatalogManager catalogManager = new CatalogManager();
        TypeManager typeManager = new TypeRegistry();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);
        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);
        AtomicReference<Boolean> metadataCreateTableCalled = new AtomicReference<>(Boolean.FALSE);

        MetadataManager metadata = new MetadataManager(
                new FeaturesConfig(),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager) {
            @Override
            public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
            {
                return Optional.empty();
            }
            @Override
            public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
            {
                metadataCreateTableCalled.set(Boolean.TRUE);
                throw new PrestoException(ALREADY_EXISTS, "Test that we ignore ALREADY_EXISTS exceptions when notExists set");
            }
        };

        AccessControl accessControl = new AccessControlManager(transactionManager);

        metadata.getTablePropertyManager().addProperties(bogusTestingCatalog.getConnectorId(), ImmutableList.of(stringSessionProperty(
                "baz",
                "test property",
                null,
                false)));
        catalogManager.registerCatalog(bogusTestingCatalog);

        CreateTable statement = new CreateTable(QualifiedName.of("test_table"),
                ImmutableList.of(new ColumnDefinition("a", "BIGINT", Optional.empty())),
                true,
                ImmutableMap.of(),
                Optional.empty());

        Session session = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();

        QueryStateMachine stateMachine = QueryStateMachine.begin(new QueryId("create_table"), "CREATE TABLE", session, URI.create("fake://uri"), true, transactionManager, accessControl, executor, metadata);

        assertTrue(stateMachine.getSession().getTransactionId().isPresent());
        assertEquals(transactionManager.getAllTransactionInfos().size(), 1);

        getFutureValue(new CreateTableTask().execute(statement, transactionManager, metadata, new AllowAllAccessControl(), stateMachine, emptyList()));
        // Success if no exception is thrown
        assertTrue(metadataCreateTableCalled.get());
    }
}
