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

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OraclePoolConnectionFactory
        extends DriverConnectionFactory
{
    private final DataSource dataSource;

    public OraclePoolConnectionFactory(Driver driver, BaseJdbcConfig config, OracleConfig oracleConfig, Properties connectionProperties)
            throws SQLException
    {
        super(driver, config);
        PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();

        //Setting connection properties of the data source
        dataSource.setConnectionFactoryClassName(OracleDataSource.class.getName());
        dataSource.setURL(config.getConnectionUrl());

        //Setting pool properties
        dataSource.setInitialPoolSize(oracleConfig.getConnectionPoolMinSize());
        dataSource.setMinPoolSize(oracleConfig.getConnectionPoolMinSize());
        dataSource.setMaxPoolSize(oracleConfig.getConnectionPoolMaxSize());
        dataSource.setValidateConnectionOnBorrow(true);
        dataSource.setConnectionProperties(connectionProperties);
        dataSource.setInactiveConnectionTimeout(toIntExact(oracleConfig.getInactiveConnectionTimeout().roundTo(SECONDS)));
        dataSource.setConnectionWaitTimeout(toIntExact(oracleConfig.getConnectionPoolWaitDuration().roundTo(SECONDS)));

        if (config.getConnectionUser() != null) {
            dataSource.setUser(config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            dataSource.setPassword(config.getConnectionPassword());
        }
        this.dataSource = dataSource;
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = dataSource.getConnection();
        // Oracle's pool doesn't reset autocommit state of connections when reusing them so we explicitly enable
        // autocommit by default to match the JDBC specification.
        connection.setAutoCommit(true);
        return connection;
    }
}
