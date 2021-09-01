package com.facebook.presto.plugin.clickhouse;
/*
 * ----------------------------------------------------------------------
 * Copyright © 2014-2021 China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * The programs can not be copied and/or distributed without the express
 * permission of China Mobile (SuZhou) Software Technology Co.,Ltd.
 *
 * ----------------------------------
 */

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSession;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author ahern
 * @date 2021/8/31 15:47
 * @since 1.0
 */
public class ClickhouseClient extends BaseJdbcClient {


    private static final Logger log = Logger.get(ClickhouseClient.class);

    @Inject
    public ClickhouseClient(JdbcConnectorId connectorId, BaseJdbcConfig config) {
        super(connectorId, config, "\"", connectionFactory(config));
        log.info("Create a Clickhouse Client");
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config) {
        Properties properties = new Properties();
        if (!Objects.isNull(config.getConnectionUser())) {
            properties.setProperty("user", config.getConnectionUser());
        }
        if (!Objects.isNull(config.getConnectionPassword())) {
            properties.setProperty("password", config.getConnectionPassword());
        }

        return new DriverConnectionFactory(new BalancedClickhouseDriver(config.getConnectionUrl(), properties), config);
    }


    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles) throws SQLException {
        return new ClickhouseQueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }
}
