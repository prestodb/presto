package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MySqlMetadata
        extends JdbcMetadata
{
    MySqlClient mySqlClient;

    public MySqlMetadata(JdbcMetadataCache jdbcMetadataCache, MySqlClient client, boolean allowDropTable, TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, client, allowDropTable, tableLocationProvider);
        mySqlClient = requireNonNull(client, "client is null");
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tableNames;
        boolean isInformationSchemaQuery = false;

        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(
                    new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            isInformationSchemaQuery = true;
            if (prefix.getSchemaName() != null) {
                tableNames = listViews(session, Optional.of(prefix.getSchemaName()));
            }
            else {
                tableNames = mySqlClient.listSchemasForViews(session);
            }
        }

        if (session.getQueryType().isPresent() || isInformationSchemaQuery) {
            return mySqlClient.getViews(session, tableNames);
        }
        return super.getViews(session, prefix);
    }
}
