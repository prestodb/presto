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
package com.facebook.presto.password.jdbc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

@Singleton
public class PostgresDatastore
        implements Datastore
{
    private BasicDataSource dataSource;
    private JdbcConfig jdbcConfig;

    @Inject
    public PostgresDatastore(JdbcConfig jdbcConfig)
    {
        this.jdbcConfig = jdbcConfig;
        initDataSource();
    }

    private void initDataSource()
    {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(jdbcConfig.getJdbcAuthUrl());
        dataSource.setUsername(jdbcConfig.getJdbcAuthUser());
        dataSource.setPassword(jdbcConfig.getJdbcAuthPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(10);
        dataSource.setMaxOpenPreparedStatements(100);

        this.dataSource = dataSource;
    }

    public Connection getConnection()
            throws SQLException
    {
        return dataSource.getConnection();
    }
}
