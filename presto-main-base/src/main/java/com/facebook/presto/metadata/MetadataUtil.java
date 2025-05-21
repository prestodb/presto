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
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.GrantorSpecification;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.PrincipalSpecification;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static final String likeTableCatalogError = "LIKE table catalog '%s' does not exist";
    public static final String catalogError = "Catalog %s does not exist";
    public static final String targetTableCatalogError = "Target catalog '%s' does not exist";
    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static SchemaTableName toSchemaTableName(QualifiedObjectName qualifiedObjectName)
    {
        return new SchemaTableName(qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
    }

    public static SchemaTableName toSchemaTableName(String schemaName, String tableName)
    {
        if (schemaName.equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return new SchemaTableName(schemaName.toLowerCase(ENGLISH), tableName.toLowerCase(ENGLISH));
        }
        return new SchemaTableName(schemaName, tableName);
    }

    public static ConnectorId getConnectorIdOrThrow(Session session, Metadata metadata, String catalogName)
    {
        return metadata.getCatalogHandle(session, catalogName)
                .orElseThrow(() -> new SemanticException(MISSING_CATALOG, "Catalog does not exist: " + catalogName));
    }

    public static ConnectorId getConnectorIdOrThrow(Session session, Metadata metadata, String catalogName, Statement statement, String errorMsg)
    {
        return metadata.getCatalogHandle(session, catalogName)
                .orElseThrow(() -> new SemanticException(MISSING_CATALOG, statement, errorMsg, catalogName));
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

    public static CatalogSchemaName createCatalogSchemaName(Session session, Node node, Optional<QualifiedName> schema, Metadata metadata)
    {
        String catalogName = session.getCatalog().orElse(null);
        String schemaName = session.getSchema().orElse(null);

        if (schema.isPresent()) {
            List<String> parts = schema.get().getOriginalParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, node, "Too many parts in schema name: %s", schema.get());
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            if (catalogName == null) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }
            schemaName = metadata.normalizeIdentifier(session, catalogName, schema.get().getOriginalSuffix());
        }

        if (catalogName == null) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
        }
        if (schemaName == null) {
            throw new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set");
        }

        return new CatalogSchemaName(catalogName, schemaName);
    }

    public static QualifiedObjectName createQualifiedObjectName(Session session, Node node, QualifiedName name, Metadata metadata)
    {
        requireNonNull(session, "session is null");
        requireNonNull(name, "name is null");
        if (name.getOriginalParts().size() > 3) {
            throw new PrestoException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getOriginalParts());
        String objectName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema().orElseThrow(() ->
                new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog().orElseThrow(() ->
                new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set"));

        catalogName = catalogName.toLowerCase(ENGLISH);
        schemaName = metadata.normalizeIdentifier(session, catalogName, schemaName);
        objectName = metadata.normalizeIdentifier(session, catalogName, objectName);

        return new QualifiedObjectName(catalogName, schemaName, objectName);
    }

    public static QualifiedName createQualifiedName(QualifiedObjectName name)
    {
        return QualifiedName.of(name.getCatalogName(), name.getSchemaName(), name.getObjectName());
    }

    public static Optional<CatalogMetadata> getOptionalCatalogMetadata(Session session, TransactionManager transactionManager, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(session.getRequiredTransactionId(), catalogName);
    }

    public static Optional<TableHandle> getOptionalTableHandle(Session session, TransactionManager transactionManager, QualifiedObjectName table, Optional<ConnectorTableVersion> tableVersion)
    {
        requireNonNull(table, "table is null");

        Optional<CatalogMetadata> catalog = getOptionalCatalogMetadata(session, transactionManager, table.getCatalogName());
        if (catalog.isPresent()) {
            CatalogMetadata catalogMetadata = catalog.get();
            ConnectorId connectorId = catalogMetadata.getConnectorId(session, table);
            ConnectorMetadata metadata = catalogMetadata.getMetadataFor(connectorId);

            ConnectorTableHandle tableHandle;

            tableHandle = tableVersion
                    .map(expression -> metadata.getTableHandle(session.toConnectorSession(connectorId), toSchemaTableName(table), Optional.of(expression)))
                    .orElseGet(() -> metadata.getTableHandle(session.toConnectorSession(connectorId), toSchemaTableName(table)));
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
        return metadata.getMetadataResolver(session).getTableHandle(name).isPresent();
    }

    public static class SchemaMetadataBuilder
    {
        public static SchemaMetadataBuilder schemaMetadataBuilder()
        {
            return new SchemaMetadataBuilder();
        }

        private final ImmutableMap.Builder<SchemaTableName, ConnectorTableMetadata> tables = ImmutableMap.builder();

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
        public static TableMetadataBuilder tableMetadataBuilder(String schemaName, String tableName)
        {
            return new TableMetadataBuilder(new SchemaTableName(schemaName, tableName));
        }

        public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName)
        {
            return new TableMetadataBuilder(tableName);
        }

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

        public TableMetadataBuilder column(String columnName, Type type)
        {
            columns.add(ColumnMetadata.builder()
                    .setName(columnName)
                    .setType(type)
                    .build());
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
