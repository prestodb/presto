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
package com.facebook.presto.verifier.framework;

import org.jdbi.v3.core.mapper.Nested;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import static java.util.Objects.requireNonNull;

public class SourceQuery
{
    private final String suite;
    private final String name;
    private final String controlQuery;
    private final String testQuery;
    private final QueryConfiguration controlConfiguration;
    private final QueryConfiguration testConfiguration;

    @JdbiConstructor
    public SourceQuery(
            @ColumnName("suite") String suite,
            @ColumnName("name") String name,
            @ColumnName("controlQuery") String controlQuery,
            @ColumnName("testQuery") String testQuery,
            @Nested("control") QueryConfiguration controlConfiguration,
            @Nested("test") QueryConfiguration testConfiguration)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.name = requireNonNull(name, "name is null");
        this.controlQuery = clean(controlQuery);
        this.testQuery = clean(testQuery);
        this.controlConfiguration = requireNonNull(controlConfiguration, "controlConfiguration is null");
        this.testConfiguration = requireNonNull(testConfiguration, "testConfiguration is null");
    }

    public String getSuite()
    {
        return suite;
    }

    public String getName()
    {
        return name;
    }

    public String getControlQuery()
    {
        return controlQuery;
    }

    public String getTestQuery()
    {
        return testQuery;
    }

    public QueryConfiguration getControlConfiguration()
    {
        return controlConfiguration;
    }

    public QueryConfiguration getTestConfiguration()
    {
        return testConfiguration;
    }

    private static String clean(String sql)
    {
        sql = sql.replaceAll("\t", "  ");
        sql = sql.replaceAll("\n+", "\n");
        sql = sql.trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
