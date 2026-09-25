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
package com.facebook.presto.ducklake.catalog.postgresql;

import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.jdbc.DuckLakeConnectionFactory;
import com.facebook.presto.ducklake.catalog.jdbc.JdbcDuckLakeCatalog;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Installed by {@link com.facebook.presto.ducklake.DuckLakeCatalogModule} when
 * {@code ducklake.catalog.type=POSTGRESQL}. Binds the JDBC-backed {@link DuckLakeCatalog}
 * implementation against a PostgreSQL catalog database.
 */
public class PostgreSqlCatalogModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(DuckLakeConnectionFactory.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeCatalog.class).to(JdbcDuckLakeCatalog.class).in(Scopes.SINGLETON);
    }
}
