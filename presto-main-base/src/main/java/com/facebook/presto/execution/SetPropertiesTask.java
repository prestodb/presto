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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetProperties;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.tree.SetProperties.Type.TABLE;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;

public class SetPropertiesTask
        implements DDLDefinitionTask<SetProperties>
{
    @Override
    public String getName()
    {
        return "SET PROPERTIES";
    }

    @Override
    public ListenableFuture<?> execute(SetProperties statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = MetadataUtil.createQualifiedObjectName(session, statement, statement.getTableName(), metadata);
        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());

        if (statement.getType() == TABLE) {
            Map<String, Object> properties = metadata.getTablePropertyManager().getUserSpecifiedProperties(
                    getConnectorIdOrThrow(session, metadata, tableName.getCatalogName()),
                    tableName.getCatalogName(),
                    sqlProperties,
                    session,
                    metadata,
                    parameterExtractor(statement, parameters)).build();
            setTableProperties(statement, tableName, metadata, accessControl, session, properties);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported target type: %s", statement.getType()));
        }

        return immediateFuture(null);
    }

    private void setTableProperties(SetProperties statement, QualifiedObjectName tableName, Metadata metadata, AccessControl accessControl, Session session, Map<String, Object> properties)
    {
        if (metadata.getMetadataResolver(session).getMaterializedView(tableName).isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot set table properties to a materialized view");
        }

        if (metadata.getMetadataResolver(session).getView(tableName).isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot set table properties to a view");
        }

        Optional<TableHandle> tableHandle = metadata.getMetadataResolver(session).getTableHandle(tableName);
        if (!tableHandle.isPresent()) {
            if (!statement.isTableExists()) {
                throw new PrestoException(NOT_FOUND, format("Table does not exist: %s", tableName));
            }
            return;
        }

        accessControl.checkCanSetTableProperties(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName, properties);
        metadata.setTableProperties(session, tableHandle.get(), properties);
    }
}
