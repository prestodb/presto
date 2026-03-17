package com.facebook.presto.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;

public class MySqlMetadataFactory
    extends JdbcMetadataFactory
{
    public MySqlMetadataFactory(JdbcMetadataCache jdbcMetadataCache, MySqlClient jdbcClient, JdbcMetadataConfig config, TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, jdbcClient, config, tableLocationProvider);
    }

    @Override
    public JdbcMetadata create()
    {
        return new MySqlMetadata(
                jdbcMetadataCache,
                (MySqlClient) jdbcClient,
                allowDropTable,
                tableLocationProvider);
    }
}
