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
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GrantorSpecification;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.PrincipalSpecification;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkCatalogName(catalogName);
        schemaName.ifPresent(name -> checkLowerCase(name, "schemaName"));
        tableName.ifPresent(name -> checkLowerCase(name, "tableName"));

        checkArgument(schemaName.isPresent() || !tableName.isPresent(), "tableName specified but schemaName is missing");
    }

    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static SchemaTableName toSchemaTableName(QualifiedObjectName qualifiedObjectName)
    {
        return new SchemaTableName(qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
    }

    public static String checkLowerCase(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
        return value;
    }

    public static ColumnMetadata findColumnMetadata(ConnectorTableMetadata tableMetadata, String columnName)
    {
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnName.equals(columnMetadata.getName())) {
                return columnMetadata;
            }
        }
        return null;
    }

    public static String createCatalogName(Session session, Node node)
    {
        Optional<String> sessionCatalog = session.getCatalog();

        if (!sessionCatalog.isPresent()) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Session catalog must be set");
        }

        return sessionCatalog.get();
    }

    public static CatalogSchemaName createCatalogSchemaName(Session session, Node node, Optional<QualifiedName> schema)
    {
        String catalogName = session.getCatalog().orElse(null);
        String schemaName = session.getSchema().orElse(null);

        if (schema.isPresent()) {
            List<String> parts = schema.get().getParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, node, "Too many parts in schema name: %s", schema.get());
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.get().getSuffix();
        }

        if (catalogName == null) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
        }
        if (schemaName == null) {
            throw new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set");
        }

        return new CatalogSchemaName(catalogName, schemaName);
    }

    public static QualifiedObjectName createQualifiedObjectName(Session session, Node node, QualifiedName name)
    {
        requireNonNull(session, "session is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new PrestoException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema().orElseThrow(() ->
                new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog().orElseThrow(() ->
                new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set"));

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }

    public static TableHandle getTableHandle(Session session, Metadata metadata, QualifiedObjectName tableName)
    {
        return metadata.getTableHandle(session, tableName).orElseThrow(() -> new SemanticException(MISSING_TABLE, "Table '%s' does not exist", tableName));
    }

    public static TableHandle getAnalyzerTableHandle(
            Session session,
            Metadata metadata,
            Map<NodeRef<Parameter>, Expression> parameters,
            QualifiedObjectName tableName,
            List<Property> properties)
    {
        ConnectorId connectorId = metadata.getCatalogHandle(session, tableName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog not found: " + tableName.getCatalogName()));

        Map<String, Object> analyzeProperties = metadata.getAnalyzePropertyManager().getProperties(
                connectorId,
                connectorId.getCatalogName(),
                mapFromProperties(properties),
                session,
                metadata,
                parameters);
        return metadata.getTableHandleForStatisticsCollection(session, tableName, analyzeProperties)
                .orElseThrow(() -> (new SemanticException(MISSING_TABLE, "Table '%s' does not exist", tableName)));
    }

    public static QualifiedName createQualifiedName(QualifiedObjectName name)
    {
        return QualifiedName.of(name.getCatalogName(), name.getSchemaName(), name.getObjectName());
    }

    public static Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, TransactionManager transactionManager, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
    }

    public static Optional<TableHandle> getOptionalTableHandle(Session session, TransactionManager transactionManager, QualifiedObjectName table)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorId connectorId = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(connectorId), toSchemaTableName(table));
            if (tableHandle != null) {
                return Optional.of(new TableHandle(
                        connectorId,
                        tableHandle,
                        catalogMetadata.getTransactionHandleFor(connectorId),
                        Optional.empty()));
            }
        }
        return Optional.empty();
    }

    public static PrestoPrincipal createPrincipal(Session session, GrantorSpecification specification)
    {
        GrantorSpecification.Type type = specification.getType();
        switch (type) {
            case PRINCIPAL:
                return createPrincipal(specification.getPrincipal().get());
            case CURRENT_USER:
                return new PrestoPrincipal(USER, session.getIdentity().getUser());
            case CURRENT_ROLE:
                // TODO: will be implemented once the "SET ROLE" statement is introduced
                throw new UnsupportedOperationException("CURRENT_ROLE is not yet supported");
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static PrestoPrincipal createPrincipal(PrincipalSpecification specification)
    {
        PrincipalSpecification.Type type = specification.getType();
        switch (type) {
            case UNSPECIFIED:
            case USER:
                return new PrestoPrincipal(USER, specification.getName().getValue());
            case ROLE:
                return new PrestoPrincipal(ROLE, specification.getName().getValue());
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static boolean tableExists(Metadata metadata, Session session, String table)
    {
        if (!session.getCatalog().isPresent() || !session.getSchema().isPresent()) {
            return false;
        }
        QualifiedObjectName name = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), table);
        return metadata.getTableHandle(session, name).isPresent();
    }

    public static class SchemaMetadataBuilder
    {
        private final ImmutableMap.Builder<SchemaTableName, ConnectorTableMetadata> tables = ImmutableMap.builder();

        public static SchemaMetadataBuilder schemaMetadataBuilder()
        {
            return new SchemaMetadataBuilder();
        }

        public SchemaMetadataBuilder table(ConnectorTableMetadata tableMetadata)
        {
            tables.put(tableMetadata.getTable(), tableMetadata);
            return this;
        }

        public ImmutableMap<SchemaTableName, ConnectorTableMetadata> build()
        {
            return tables.build();
        }
    }

    public static class TableMetadataBuilder
    {
        private final SchemaTableName tableName;
        private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        private final Optional<String> comment;
        private TableMetadataBuilder(SchemaTableName tableName)
        {
            this(tableName, Optional.empty());
        }
        private TableMetadataBuilder(SchemaTableName tableName, Optional<String> comment)
        {
            this.tableName = tableName;
            this.comment = comment;
        }

        public static TableMetadataBuilder tableMetadataBuilder(String schemaName, String tableName)
        {
            return new TableMetadataBuilder(new SchemaTableName(schemaName, tableName));
        }

        public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName)
        {
            return new TableMetadataBuilder(tableName);
        }

        public TableMetadataBuilder column(String columnName, Type type)
        {
            columns.add(new ColumnMetadata(columnName, type));
            return this;
        }

        public TableMetadataBuilder property(String name, Object value)
        {
            properties.put(name, value);
            return this;
        }

        public ConnectorTableMetadata build()
        {
            return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment);
        }
    }
}
