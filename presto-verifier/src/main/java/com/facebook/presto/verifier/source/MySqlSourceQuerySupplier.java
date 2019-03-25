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
package com.facebook.presto.verifier.source;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;

import java.sql.DriverManager;

public class MySqlSourceQuerySupplier
        extends AbstractJdbiSourceQuerySupplier
{
    public static final String MYSQL_SOURCE_QUERY_SUPPLIER = "mysql";

    @Inject
    public MySqlSourceQuerySupplier(MySqlSourceQueryConfig config)
    {
        super(() -> Jdbi.create(() -> DriverManager.getConnection(config.getDatabase())).installPlugin(new SqlObjectPlugin()),
                config.getTableName(),
                config.getSuites(),
                config.getMaxQueriesPerSuite());
    }
}
