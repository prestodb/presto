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
package com.facebook.presto.plugin.turbonium.config.db;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.facebook.presto.plugin.turbonium.config.db.DatabaseUtil.onDemandDao;
import static java.util.Objects.requireNonNull;

public class MySqlDaoProvider
    implements Provider<TurboniumConfigDao>
{
    private final TurboniumConfigDao dao;

    @Inject
    public MySqlDaoProvider(TurboniumDbConfig dbConfig)
    {
        requireNonNull(dbConfig, "dbConfig is null");
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(dbConfig.getConfigDbUrl());
        IDBI dbi = new DBI(dataSource);
        this.dao = onDemandDao(dbi, TurboniumConfigDao.class);
    }

    @Override
    public TurboniumConfigDao get()
    {
        return dao;
    }
}
