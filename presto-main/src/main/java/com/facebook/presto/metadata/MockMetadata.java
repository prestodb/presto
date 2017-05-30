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
package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class MockMetadata implements Metadata
{
    private int createTableCallCount = 0; // not thread-safe
    private ConnectorId catalogHandle = null;
    private RuntimeException createTableException = null;
    private TableHandle tableHandle = null;
    private TablePropertyManager tablePropertyManager = null;
    private TypeManager typeManager = null;

    @Override
    public void verifyComparableOrderableContract()
    {
    }

    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeManager.getType(signature);
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return false;
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        return null;
    }

    @Override
    public void addFunctions(List<? extends SqlFunction> functions)
    {
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        return false;
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        return null;
    }

    public void setTableHandle(TableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        return Optional.ofNullable(this.tableHandle);
    }

    @Override
    public List<TableLayoutResult> getLayouts(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return null;
    }

    @Override
    public TableLayout getLayout(Session session, TableLayoutHandle handle)
    {
        return null;
    }

    @Override
    public Optional<Object> getInfo(Session session, TableLayoutHandle handle)
    {
        return null;
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        return null;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        return null;
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
    }

    public void setCreateTableException(RuntimeException e)
    {
        this.createTableException = e;
    }

    public int getCreateTableCallCount()
    {
        return this.createTableCallCount;
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        ++this.createTableCallCount;
        if (this.createTableException != null) {
            throw this.createTableException;
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
    }

    @Override
    public TableIdentity getTableIdentity(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public TableIdentity deserializeTableIdentity(Session session, String catalogName, byte[] bytes)
    {
        return null;
    }

    @Override
    public ColumnIdentity getColumnIdentity(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public ColumnIdentity deserializeColumnIdentity(Session session, String catalogName, byte[] bytes)
    {
        return null;
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return null;
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        return null;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return null;
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        return null;
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
    }

    @Override
    public void cleanupQuery(Session session)
    {
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        return null;
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        return false;
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        return null;
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
    }

    public void setCatalogHandle(ConnectorId catalogHandle)
    {
        this.catalogHandle = catalogHandle;
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        return Optional.ofNullable(this.catalogHandle);
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        return null;
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        return null;
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        return null;
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        return null;
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, String viewData, boolean replace)
    {
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return null;
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return null;
    }

    @Override
    public FunctionRegistry getFunctionRegistry()
    {
        return null;
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        return null;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return null;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return null;
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return null;
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        return null;
    }

    public void setTablePropertyManager(TablePropertyManager tablePropertyManager)
    {
        this.tablePropertyManager = tablePropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return this.tablePropertyManager;
    }
}
