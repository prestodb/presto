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
package com.facebook.presto.session.db;

import com.facebook.presto.session.db.util.DataSourceFactory;
import com.facebook.presto.session.db.util.MariaDBDataSourceFactory;
import com.facebook.presto.session.db.util.MySQLDataSourceFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class SessionPropertiesDaoProvider
        implements Provider<SessionPropertiesDao>
{
    private final SessionPropertiesDao dao;

    @Inject
    public SessionPropertiesDaoProvider(DbSessionPropertyManagerConfig config)
    {
        requireNonNull(config, "config is null");
        requireNonNull(config.getConfigDbUrl(), "db url is null");
        requireNonNull(config.getJdbcDataSource(), "jdbc data source is null");
        DataSourceFactory dataSourceFactory = null;
        switch (config.getJdbcDataSource()) {
            case "mysql":
                dataSourceFactory = new MySQLDataSourceFactory();
                break;
            case "mariadb":
                dataSourceFactory = new MariaDBDataSourceFactory();
                break;
            default:
                throw new RuntimeException("session-property-manager.db.database-data-source-name property has invalid value");
        }
        this.dao = Jdbi.create(dataSourceFactory.create(config))
                .installPlugin(new SqlObjectPlugin())
                .onDemand(SessionPropertiesDao.class);
    }

    @Override
    public SessionPropertiesDao get()
    {
        return dao;
    }
}
