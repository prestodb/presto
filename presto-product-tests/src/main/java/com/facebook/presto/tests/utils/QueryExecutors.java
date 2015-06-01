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
import com.teradata.tempto.query.QueryExecutor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;

public class QueryExecutors
{
    private QueryExecutors() {}

    public static QueryExecutor onPresto()
    {
        return testContext().getDependency(QueryExecutor.class, "presto");
    }

    public static void onPrestoWith(Map<String, String> sessionProperties, ConnectionConsumer connectionConsumer)
            throws SQLException
    {
        try (PrestoConnection connection = (PrestoConnection) onPresto().getConnection()) {
            sessionProperties.entrySet().stream()
                    .forEach(entry -> connection.setSessionProperty(entry.getKey(), entry.getValue()));
            connectionConsumer.accept(connection);
        }
    }

    public interface ConnectionConsumer
    {
        void accept(Connection connection)
                throws SQLException;
    }
}
