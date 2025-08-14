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
package com.facebook.presto.session.db.util;

import com.facebook.presto.session.db.DbSessionPropertyManagerConfig;
import com.mysql.cj.jdbc.MysqlDataSource;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Optional;

public class MySQLDataSourceFactory
        implements DataSourceFactory
{
    @Override
    public DataSource create(DbSessionPropertyManagerConfig config)
    {
        MysqlDataSource dataSource = new MysqlDataSource();

        dataSource.setURL(config.getConfigDbUrl());

        Optional<String> username = Optional.ofNullable(config.getUsername());
        username.ifPresent(dataSource::setUser);

        Optional<String> password = Optional.ofNullable(config.getPassword());
        password.ifPresent(dataSource::setPassword);
        try {
            dataSource.setCreateDatabaseIfNotExist(true);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to setCreateDatabaseIfNotExist");
        }
        return dataSource;
    }
}
