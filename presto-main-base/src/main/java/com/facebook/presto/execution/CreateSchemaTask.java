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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.Catalog.CatalogContext.getConnectorByCatalog;
import static com.facebook.presto.metadata.MetadataUtil.createCatalogSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.StandardWarningCode.LAKEHOUSE_CONNECTORS_MIGHT_USE_SAME_METASTORE;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class CreateSchemaTask
        implements DDLDefinitionTask<CreateSchema>
{
    @Override
    public String getName()
    {
        return "CREATE SCHEMA";
    }

    @Override
    public String explain(CreateSchema statement, List<Expression> parameters)
    {
        return "CREATE SCHEMA " + statement.getSchemaName();
    }

    @Override
    public ListenableFuture<?> execute(CreateSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()), metadata);

        // TODO: validate that catalog exists

        accessControl.checkCanCreateSchema(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), schema);

        if (metadata.getMetadataResolver(session).schemaExists(schema)) {
            if (!statement.isNotExists()) {
                String connectorName = getConnectorByCatalog().get(schema.getCatalogName());
                if (isLakehouseConnector(connectorName)) {
                    PrestoWarning warning = new PrestoWarning(LAKEHOUSE_CONNECTORS_MIGHT_USE_SAME_METASTORE, "The schema might be existing in another lakehouse " +
                            "catalog (hive/hudi/delta/iceberg) which uses the same underlying metastore. Please check the value of `hive.metastore.uri` config in corresponding catalog.properties files");
                    warningCollector.add(warning);
                }
                throw new SemanticException(SCHEMA_ALREADY_EXISTS, statement, "Schema '%s' already exists", schema);
            }
            return immediateFuture(null);
        }

        Map<String, Object> properties = metadata.getSchemaPropertyManager().getProperties(
                getConnectorIdOrThrow(session, metadata, schema.getCatalogName()),
                schema.getCatalogName(),
                mapFromProperties(statement.getProperties()),
                session,
                metadata,
                parameterExtractor(statement, parameters));

        metadata.createSchema(session, schema, properties);

        return immediateFuture(null);
    }

    private boolean isLakehouseConnector(String connectorName)
    {
        Set<String> lakehouseConnectors = ImmutableSet.of("hive-hadoop2", "hudi", "iceberg", "delta");
        return !isNullOrEmpty(connectorName) && lakehouseConnectors.contains(connectorName);
    }
}
