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

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.DriverManager;

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
        this.dao = Jdbi.create(() -> DriverManager.getConnection(config.getConfigDbUrl()))
                .installPlugin(new SqlObjectPlugin())
                .onDemand(SessionPropertiesDao.class);
    }

    @Override
    public SessionPropertiesDao get()
    {
        return dao;
    }
}
