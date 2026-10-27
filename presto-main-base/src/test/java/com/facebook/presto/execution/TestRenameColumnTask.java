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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_ALREADY_EXISTS;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.ALTER_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

@Test(singleThreaded = true)
public class TestRenameColumnTask
{
    private static final String CATALOG_NAME = "catalog";
    private static final String TABLE_NAME = "test_table";

    private TransactionManager transactionManager;
    private Session testSession;
    private MockMetadata metadata;
    private TestingAccessControlManager accessControl;
    private final TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), new TestingWarningCollectorConfig().setAddWarnings(true));

    @BeforeMethod
    public void setUp()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        Catalog testCatalog = createBogusTestingCatalog(CATALOG_NAME);
        catalogManager.registerCatalog(testCatalog);
        testSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema("schema")
                .setTransactionId(transactionManager.beginTransaction(false))
                .build();
        metadata = new MockMetadata(testCatalog.getConnectorId());
        accessControl = new TestingAccessControlManager(transactionManager);
    }

    @Test
    public void testTopLevelRenameColumnCallsRenameColumn()
    {
        execute(renameColumn(QualifiedName.of("x"), "z", false));

        assertEquals(metadata.renamedColumn, new TestingColumnHandle("x"));
        assertEquals(metadata.renamedColumnTarget, "z");
        assertEquals(metadata.renameColumnCalls, 1);
        assertEquals(metadata.renameFieldCalls, 0);
    }

    @Test
    public void testNestedRenameColumnCallsRenameField()
    {
        execute(renameColumn(QualifiedName.of("info", "age"), "years", false));

        assertEquals(metadata.receivedFieldPath, ImmutableList.of("info", "age"));
        assertEquals(metadata.receivedFieldTarget, "years");
        assertEquals(metadata.renameFieldCalls, 1);
        assertEquals(metadata.renameColumnCalls, 0);
        assertNull(metadata.renamedColumn);
    }

    @Test
    public void testNestedRenamePassesNormalizedPathAndTarget()
    {
        // MockMetadata.normalizeIdentifier lower-cases, so the connector must receive normalized parts.
        QualifiedName nestedName = QualifiedName.of(ImmutableList.of(
                new Identifier("Outer", true),
                new Identifier("Inner", true),
                new Identifier("Age", true)));

        execute(new RenameColumn(QualifiedName.of(TABLE_NAME), nestedName, new Identifier("Years", true), false, false));

        assertEquals(metadata.receivedFieldPath, ImmutableList.of("outer", "inner", "age"));
        assertEquals(metadata.receivedFieldTarget, "years");
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testNestedRenameRequiresRenameColumnPermission()
    {
        // Unlike nested ADD/DROP, which require ALTER_COLUMN, nested RENAME uses RENAME_COLUMN like a top-level rename.
        accessControl.deny(privilege(TABLE_NAME, RENAME_COLUMN));
        assertThrows(AccessDeniedException.class, () -> execute(renameColumn(QualifiedName.of("info", "age"), "years", false)));
        // A denied statement must not reach the metadata layer
        assertEquals(metadata.renameFieldCalls, 0);
        assertEquals(metadata.renameColumnCalls, 0);
    }

    @Test
    public void testNestedRenameAllowedWhenOnlyAlterColumnDenied()
    {
        accessControl.deny(privilege(TABLE_NAME, ALTER_COLUMN));
        execute(renameColumn(QualifiedName.of("info", "age"), "years", false));

        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testTopLevelRenameDeniedWhenRenameColumnNotAllowed()
    {
        accessControl.deny(privilege(TABLE_NAME, RENAME_COLUMN));
        assertThrows(AccessDeniedException.class, () -> execute(renameColumn(QualifiedName.of("x"), "z", false)));
        assertEquals(metadata.renameColumnCalls, 0);
        assertEquals(metadata.renameFieldCalls, 0);
    }

    @Test
    public void testNestedRenameFieldNotFoundFailsWithoutIfExists()
    {
        metadata.renameFieldFailure = new PrestoException(COLUMN_NOT_FOUND, "Field 'info.age' does not exist");

        assertThrows(PrestoException.class, () -> execute(renameColumn(QualifiedName.of("info", "age"), "years", false)));
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testNestedRenameFieldNotFoundSuppressedByIfExists()
    {
        metadata.renameFieldFailure = new PrestoException(COLUMN_NOT_FOUND, "Field 'info.age' does not exist");

        execute(renameColumn(QualifiedName.of("info", "age"), "years", true));
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testIfExistsSuppressesBadIntermediateSegment()
    {
        // RENAME COLUMN IF EXISTS aress.zip TO postal_code — "aress" does not exist.
        // IcebergAbstractMetadata.resolveFieldToRename throws COLUMN_NOT_FOUND for a missing
        // intermediate segment; IF EXISTS must suppress it silently.
        metadata.renameFieldFailure = new PrestoException(COLUMN_NOT_FOUND, "Cannot find parent field 'aress' in table 'test_table'");

        execute(renameColumn(QualifiedName.of("aress", "zip"), "postal_code", true));
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testIfExistsSuppressesBadLeafField()
    {
        // RENAME COLUMN IF EXISTS address.ziip TO postal_code — "ziip" leaf does not exist.
        // Also throws COLUMN_NOT_FOUND; IF EXISTS must suppress it silently.
        metadata.renameFieldFailure = new PrestoException(COLUMN_NOT_FOUND, "Field 'address.ziip' does not exist in table 'test_table'");

        execute(renameColumn(QualifiedName.of("address", "ziip"), "postal_code", true));
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testIfExistsDoesNotSuppressOtherNestedRenameFailures()
    {
        // Only COLUMN_NOT_FOUND is swallowed by IF EXISTS; any other connector failure must surface.
        metadata.renameFieldFailure = new PrestoException(com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED, "This connector does not support renaming nested fields");

        assertThrows(PrestoException.class, () -> execute(renameColumn(QualifiedName.of("info", "age"), "years", true)));
        assertEquals(metadata.renameFieldCalls, 1);
    }

    @Test
    public void testTopLevelIfExistsSkipsMissingColumn()
    {
        execute(renameColumn(QualifiedName.of("missing"), "z", true));

        assertEquals(metadata.renameColumnCalls, 0);
        assertEquals(metadata.renameFieldCalls, 0);
    }

    @Test
    public void testTopLevelRenameTargetAlreadyExistsFails()
    {
        SemanticException e = org.testng.Assert.expectThrows(SemanticException.class, () -> execute(renameColumn(QualifiedName.of("x"), "y", false)));
        assertEquals(e.getCode(), COLUMN_ALREADY_EXISTS);
        assertEquals(metadata.renameColumnCalls, 0);
    }

    @Test
    public void testTopLevelRenameHiddenColumnFails()
    {
        SemanticException e = org.testng.Assert.expectThrows(SemanticException.class, () -> execute(renameColumn(QualifiedName.of("hidden"), "z", false)));
        assertEquals(e.getCode(), com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED);
        assertEquals(metadata.renameColumnCalls, 0);
    }

    @Test
    public void testNestedRenameCasePreservingNormalizer()
    {
        metadata.preserveCase = true;
        QualifiedName nestedName = QualifiedName.of(ImmutableList.of(
                new Identifier("Outer", true),
                new Identifier("Inner", true),
                new Identifier("Age", true)));

        execute(new RenameColumn(QualifiedName.of(TABLE_NAME), nestedName, new Identifier("Years", true), false, false));

        assertEquals(metadata.receivedFieldPath, ImmutableList.of("Outer", "Inner", "Age"));
        assertEquals(metadata.receivedFieldTarget, "Years");
        assertEquals(metadata.renameFieldCalls, 1);
    }

    private static RenameColumn renameColumn(QualifiedName source, String target, boolean columnExists)
    {
        return new RenameColumn(QualifiedName.of(TABLE_NAME), source, new Identifier(target), false, columnExists);
    }

    private void execute(RenameColumn statement)
    {
        getFutureValue(new RenameColumnTask().execute(statement, transactionManager, metadata, accessControl, testSession, emptyList(), warningCollector, ""));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        private final ConnectorId catalogHandle;
        private final TableHandle tableHandle;
        private ColumnHandle renamedColumn;
        private String renamedColumnTarget;
        private int renameColumnCalls;
        private List<String> receivedFieldPath;
        private String receivedFieldTarget;
        private int renameFieldCalls;
        private PrestoException renameFieldFailure;
        private boolean preserveCase;

        public MockMetadata(ConnectorId catalogHandle)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.tableHandle = new TableHandle(catalogHandle, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty());
        }

        @Override
        public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
        {
            this.renamedColumn = source;
            this.renamedColumnTarget = target;
            this.renameColumnCalls++;
        }

        @Override
        public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
        {
            this.receivedFieldPath = fieldPath;
            this.receivedFieldTarget = target;
            this.renameFieldCalls++;
            if (renameFieldFailure != null) {
                throw renameFieldFailure;
            }
        }

        @Override
        public String normalizeIdentifier(Session session, String catalogName, String identifier)
        {
            if (preserveCase) {
                return identifier;
            }
            return identifier.toLowerCase(Locale.ENGLISH);
        }

        @Override
        public MetadataResolver getMetadataResolver(Session session)
        {
            return new MetadataResolver()
            {
                @Override
                public boolean catalogExists(String catalogName)
                {
                    return catalogHandle.getCatalogName().equals(catalogName);
                }

                @Override
                public boolean schemaExists(CatalogSchemaName schemaName)
                {
                    return true;
                }

                @Override
                public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
                {
                    return Optional.of(tableHandle);
                }

                @Override
                public List<ColumnMetadata> getColumns(TableHandle tableHandle)
                {
                    return emptyList();
                }

                @Override
                public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
                {
                    return ImmutableMap.of();
                }

                @Override
                public Optional<ViewDefinition> getView(QualifiedObjectName viewName)
                {
                    return Optional.empty();
                }

                @Override
                public Optional<MaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName)
                {
                    return Optional.empty();
                }
            };
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
        {
            return ImmutableMap.of(
                    "x", new TestingColumnHandle("x"),
                    "y", new TestingColumnHandle("y"),
                    "hidden", new TestingColumnHandle("hidden"));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            String name = ((TestingColumnHandle) columnHandle).getName();
            return ColumnMetadata.builder()
                    .setName(name)
                    .setType(BIGINT)
                    .setHidden("hidden".equals(name))
                    .build();
        }
    }
}
