
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
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.MockMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

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
        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);

        TypeManager typeManager = new TypeRegistry();
        TransactionManager transactionManager = createTestTransactionManager(catalogManager);

        TablePropertyManager tablePropertyManager = new TablePropertyManager();
        tablePropertyManager.addProperties(bogusTestingCatalog.getConnectorId(), ImmutableList.of(stringSessionProperty(
                "baz",
                "test property",
                null,
                false)));

        MockMetadata metadata = new MockMetadata();
        RuntimeException createTableException =
                new PrestoException(ALREADY_EXISTS, "Test that we ignore ALREADY_EXISTS exceptions when notExists set");
        metadata.setTypeManager(typeManager);
        metadata.setCreateTableException(createTableException);
        metadata.setTablePropertyManager(tablePropertyManager);
        metadata.setCatalogHandle(bogusTestingCatalog.getConnectorId());

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

        getFutureValue(new CreateTableTask().internalExecute(statement, metadata, new AllowAllAccessControl(), session, emptyList()));
        // Success if no exception is thrown
        assertEquals(metadata.getCreateTableCallCount(), 1);
    }
}
