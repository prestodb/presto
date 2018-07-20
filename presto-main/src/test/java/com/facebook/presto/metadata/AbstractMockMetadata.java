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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
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

public abstract class AbstractMockMetadata
        implements Metadata
{
    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFunctions(List<? extends SqlFunction> functions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableLayoutResult> getLayouts(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableLayout getLayout(Session session, TableLayoutHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> getInfo(Session session, TableLayoutHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beginQuery(Session session, Set<ConnectorId> connectors)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupQuery(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong metadataDelete(Session session, TableHandle tableHandle, TableLayoutHandle tableLayoutHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorId> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, String viewData, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionRegistry getFunctionRegistry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeManager getTypeManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }
}
