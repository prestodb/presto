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
package com.facebook.presto.tests.utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.facebook.presto.tests.utils.QueryExecutors.onPrestoWith;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;

public final class PrestoDDLUtils
{
    private PrestoDDLUtils() {}

    public static Table createPrestoTable(String tableName, String tableDDL)
    {
        return createPrestoTable(tableName, tableDDL, emptyMap());
    }

    public static Table createPrestoTable(String tableName, String tableDDL, Map<String, String> sessionProperties)
    {
        Table table = new Table(tableName, tableDDL, sessionProperties);
        try {
            table.executeCreateTable();
        }
        catch (SQLException e) {
            throw new IllegalArgumentException("Can't create presto table " + tableName, e);
        }
        return table;
    }

    public static class Table
            implements Closeable
    {
        private final String tableName;
        private final String tableDDL;
        private final Map<String, String> sessionProperties;

        public Table(String tableName, String tableDDL, Map<String, String> sessionProperties)
        {
            this.tableName = tableName;
            this.tableDDL = tableDDL;
            this.sessionProperties = sessionProperties;
        }

        public String getNameInDatabase()
        {
            return tableName;
        }

        @Override
        public void close()
                throws IOException
        {
            executeDropTable();
        }

        private void executeCreateTable()
                throws SQLException
        {
            String createTableSQL = format(tableDDL, tableName);
            onPrestoWith(sessionProperties).executeQuery(createTableSQL);
        }

        private void executeDropTable()
        {
            String dropTableSQL = format("DROP TABLE %s", tableName);
            onPresto().executeQuery(dropTableSQL);
        }
    }
}
