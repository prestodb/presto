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
package com.facebook.presto.plugin.oracle;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OracleMetadata
        extends JdbcMetadata
{
    private static final Logger LOG = Logger.get(OracleMetadata.class);
    private final OracleClient oracleClient;

    public OracleMetadata(
            JdbcMetadataCache jdbcMetadataCache,
            OracleClient oracleClient,
            boolean allowDropTable,
            TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, oracleClient, allowDropTable, tableLocationProvider);
        this.oracleClient = oracleClient;
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        List<SchemaTableName> viewNames;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            viewNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else if (prefix.getSchemaName() != null) {
            viewNames = oracleClient.listViews(session, identity, Optional.of(prefix.getSchemaName()));
        }
        else {
            viewNames = oracleClient.listSchemasForViews(session, identity);
        }

        return oracleClient.getViews(session, identity, viewNames);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        return oracleClient.listViews(session, identity, schemaName);
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        oracleClient.createView(session, identity, viewMetadata.getTable(), viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        oracleClient.dropView(session, identity, viewName);
    }
}
