package com.facebook.presto.plugin.clickhouse;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author ahern
 * @date 2021/8/31 16:20
 * @since 1.0
 */
public class BalancedClickhouseDriver implements Driver {
    private final String url;
    private final BalancedClickhouseDataSource dataSource;

    public BalancedClickhouseDriver(final String url, Properties properties) {
        this.url = url;
        this.dataSource = new BalancedClickhouseDataSource(url, properties);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("url not accept.");
        }

        return dataSource.getConnection();
    }

    @Override
    public boolean acceptsURL(String url) {
        return this.url.equals(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }
}
