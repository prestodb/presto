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
package com.facebook.presto.resourceGroups.db;

import org.h2.jdbcx.JdbcDataSource;
import org.skife.jdbi.v2.DBI;

import javax.inject.Inject;
import javax.inject.Provider;

public class H2DaoProvider
        implements Provider<ResourceGroupsDao>
{
    private final H2ResourceGroupsDao dao;

    @Inject
    public H2DaoProvider(DbResourceGroupConfig config)
    {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(config.getConfigDbUrl());
        DBI dbi = new DBI(ds);
        this.dao = dbi.open(H2ResourceGroupsDao.class);
    }

    @Override
    public H2ResourceGroupsDao get()
    {
        return dao;
    }
}
