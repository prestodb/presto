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

import com.facebook.presto.jdbc.PrestoConnection;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.JdbcQueryExecutor;
import com.teradata.tempto.query.QueryExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;

public class QueryExecutors
{
    private static final String PRESTO_CONFIGURATION_KEY = "databases.presto";

    private QueryExecutors() {}

    public static QueryExecutor onPresto()
    {
        return testContext().getDependency(QueryExecutor.class, "presto");
    }

    public static QueryExecutor onPrestoWith(Map<String, String> sessionProperties)
            throws SQLException
    {
        PrestoConnection prestoConnection = createPrestoConnection();
        sessionProperties.entrySet().stream()
                .forEach(entry -> prestoConnection.setSessionProperty(entry.getKey(), entry.getValue()));
        return new JdbcQueryExecutor(prestoConnection, getPrestoConfiguration().getStringMandatory("jdbc_url"));
    }

    public static QueryExecutor onPrestoOnNullCatalog()
    {
        return testContext().getDependency(QueryExecutor.class, "presto_null");
    }

    public static QueryExecutor onHive()
    {
        return testContext().getDependency(QueryExecutor.class, "hive");
    }

    public static PrestoConnection createPrestoConnection()
            throws SQLException
    {
        Configuration config = getPrestoConfiguration();
        Connection connection = DriverManager.getConnection(
                config.getStringMandatory("jdbc_url"),
                config.getStringMandatory("jdbc_user"),
                config.getStringMandatory("jdbc_password")
        );

        return connection.unwrap(PrestoConnection.class);
    }

    private static Configuration getPrestoConfiguration()
    {
        return getConfiguration().getSubconfiguration(PRESTO_CONFIGURATION_KEY);
    }

    private static Configuration getConfiguration()
    {
        return testContext().getDependency(Configuration.class);
    }
}
