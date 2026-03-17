package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class MySqlMetadataFactory
{
    private final JdbcMetadataCache jdbcMetadataCache;
    private final MySqlClient mySqlClient;
    private final boolean allowDropTable;
    private final TableLocationProvider tableLocationProvider;

    @Inject
    public MySqlMetadataFactory(JdbcMetadataCache jdbcMetadataCache, MySqlClient mySqlClient, JdbcMetadataConfig config, TableLocationProvider tableLocationProvider)
    {
        this.jdbcMetadataCache = requireNonNull(jdbcMetadataCache, "jdbcMetadataCache is null");
        this.mySqlClient = requireNonNull(mySqlClient, "mySqlClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
        this.tableLocationProvider = requireNonNull(tableLocationProvider, "tableLocationProvider is null");
    }

    public MySqlMetadata create()
    {
        return new MySqlMetadata(
                jdbcMetadataCache,
                mySqlClient,
                allowDropTable,
                tableLocationProvider);
    }
}
