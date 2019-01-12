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
package io.prestosql.tests.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class JdbcSqlExecutor
        implements SqlExecutor
{
    private final String jdbcUrl;
    private final Properties jdbcProperties;

    public JdbcSqlExecutor(String jdbcUrl)
    {
        this(jdbcUrl, new Properties());
    }

    public JdbcSqlExecutor(String jdbcUrl, Properties jdbcProperties)
    {
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.jdbcProperties = new Properties();
        this.jdbcProperties.putAll(requireNonNull(jdbcProperties, "jdbcProperties is null"));
    }

    @Override
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error executing sql:\n" + sql, e);
        }
    }
}
