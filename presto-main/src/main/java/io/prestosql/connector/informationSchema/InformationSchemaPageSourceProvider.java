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
package io.prestosql.connector.informationSchema;

import com.google.common.collect.ImmutableList;
import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.metadata.InternalTable;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.metadata.ViewDefinition;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrivilegeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.Sets.union;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLE_PRIVILEGES;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.TABLE_VIEWS;
import static io.prestosql.connector.informationSchema.InformationSchemaMetadata.informationSchemaTableColumns;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataListing.listTableColumns;
import static io.prestosql.metadata.MetadataListing.listTablePrivileges;
import static io.prestosql.metadata.MetadataListing.listTables;
import static io.prestosql.metadata.MetadataListing.listViews;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class InformationSchemaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    public InformationSchemaPageSourceProvider(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        InternalTable table = getInternalTable(session, split, columns);

        List<Integer> channels = new ArrayList<>();
        for (ColumnHandle column : columns) {
            String columnName = ((InformationSchemaColumnHandle) column).getColumnName();
            int columnIndex = table.getColumnIndex(columnName);
            channels.add(columnIndex);
        }

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        for (Page page : table.getPages()) {
            Block[] blocks = new Block[channels.size()];
            for (int index = 0; index < blocks.length; index++) {
                blocks[index] = page.getBlock(channels.get(index));
            }
            pages.add(new Page(page.getPositionCount(), blocks));
        }
        return new FixedPageSource(pages.build());
    }

    private InternalTable getInternalTable(ConnectorSession connectorSession, ConnectorSplit connectorSplit, List<ColumnHandle> columns)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        InformationSchemaSplit split = (InformationSchemaSplit) connectorSplit;

        requireNonNull(columns, "columns is null");

        InformationSchemaTableHandle handle = split.getTableHandle();
        Set<QualifiedTablePrefix> prefixes = split.getPrefixes();

        return getInformationSchemaTable(session, handle.getCatalogName(), handle.getSchemaTableName(), prefixes);
    }

    public InternalTable getInformationSchemaTable(Session session, String catalog, SchemaTableName table, Set<QualifiedTablePrefix> prefixes)
    {
        if (table.equals(TABLE_COLUMNS)) {
            return buildColumns(session, prefixes);
        }
        if (table.equals(TABLE_TABLES)) {
            return buildTables(session, prefixes);
        }
        if (table.equals(TABLE_VIEWS)) {
            return buildViews(session, prefixes);
        }
        if (table.equals(TABLE_SCHEMATA)) {
            return buildSchemata(session, catalog);
        }
        if (table.equals(TABLE_TABLE_PRIVILEGES)) {
            return buildTablePrivileges(session, prefixes);
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildColumns(Session session, Set<QualifiedTablePrefix> prefixes)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_COLUMNS));
        for (QualifiedTablePrefix prefix : prefixes) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : listTableColumns(session, metadata, accessControl, prefix).entrySet()) {
                SchemaTableName tableName = entry.getKey();
                int ordinalPosition = 1;
                for (ColumnMetadata column : entry.getValue()) {
                    if (column.isHidden()) {
                        continue;
                    }
                    table.add(
                            prefix.getCatalogName(),
                            tableName.getSchemaName(),
                            tableName.getTableName(),
                            column.getName(),
                            ordinalPosition,
                            null,
                            "YES",
                            column.getType().getDisplayName(),
                            column.getComment(),
                            column.getExtraInfo());
                    ordinalPosition++;
                }
            }
        }
        return table.build();
    }

    private InternalTable buildTables(Session session, Set<QualifiedTablePrefix> prefixes)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_TABLES));
        for (QualifiedTablePrefix prefix : prefixes) {
            Set<SchemaTableName> tables = listTables(session, metadata, accessControl, prefix);
            Set<SchemaTableName> views = listViews(session, metadata, accessControl, prefix);

            for (SchemaTableName name : union(tables, views)) {
                // if table and view names overlap, the view wins
                String type = views.contains(name) ? "VIEW" : "BASE TABLE";
                table.add(
                        prefix.getCatalogName(),
                        name.getSchemaName(),
                        name.getTableName(),
                        type);
            }
        }
        return table.build();
    }

    private InternalTable buildTablePrivileges(Session session, Set<QualifiedTablePrefix> prefixes)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_TABLE_PRIVILEGES));
        for (QualifiedTablePrefix prefix : prefixes) {
            List<GrantInfo> grants = ImmutableList.copyOf(listTablePrivileges(session, metadata, accessControl, prefix));
            for (GrantInfo grant : grants) {
                for (PrivilegeInfo privilegeInfo : grant.getPrivilegeInfo()) {
                    table.add(
                            grant.getGrantor().orElse(null),
                            grant.getIdentity().getUser(),
                            prefix.getCatalogName(),
                            grant.getSchemaTableName().getSchemaName(),
                            grant.getSchemaTableName().getTableName(),
                            privilegeInfo.getPrivilege().name(),
                            privilegeInfo.isGrantOption(),
                            grant.getWithHierarchy().orElse(null));
                }
            }
        }
        return table.build();
    }

    private InternalTable buildViews(Session session, Set<QualifiedTablePrefix> prefixes)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_VIEWS));
        for (QualifiedTablePrefix prefix : prefixes) {
            for (Entry<QualifiedObjectName, ViewDefinition> entry : metadata.getViews(session, prefix).entrySet()) {
                table.add(
                        entry.getKey().getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getObjectName(),
                        entry.getValue().getOriginalSql());
            }
        }
        return table.build();
    }

    private InternalTable buildSchemata(Session session, String catalogName)
    {
        InternalTable.Builder table = InternalTable.builder(informationSchemaTableColumns(TABLE_SCHEMATA));
        for (String schema : listSchemas(session, metadata, accessControl, catalogName)) {
            table.add(catalogName, schema);
        }
        return table.build();
    }
}
