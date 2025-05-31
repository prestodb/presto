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

import com.mysql.cj.jdbc.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SessionPropertiesDaoProvider
        implements Provider<SessionPropertiesDao>
{
    private final SessionPropertiesDao dao;

    @Inject
    public SessionPropertiesDaoProvider(DbSessionPropertyManagerConfig config)
    {
        requireNonNull(config, "config is null");
        String url = requireNonNull(config.getConfigDbUrl(), "db url is null");

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(url);

        Optional<String> username = Optional.ofNullable(config.getUsername());
        username.ifPresent(dataSource::setUser);

        Optional<String> password = Optional.ofNullable(config.getPassword());
        password.ifPresent(dataSource::setPassword);

        this.dao = Jdbi.create(dataSource)
                .installPlugin(new SqlObjectPlugin())
                .onDemand(SessionPropertiesDao.class);
    }

    @Override
    public SessionPropertiesDao get()
    {
        return dao;
    }
}
