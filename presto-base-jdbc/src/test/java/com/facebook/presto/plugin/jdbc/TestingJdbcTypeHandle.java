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
package com.facebook.presto.plugin.jdbc;

import java.sql.Types;

public final class TestingJdbcTypeHandle
{
    private TestingJdbcTypeHandle() {}

    public static final JdbcTypeHandle JDBC_BOOLEAN = new JdbcTypeHandle(Types.BOOLEAN, "boolean", 1, 0);

    public static final JdbcTypeHandle JDBC_SMALLINT = new JdbcTypeHandle(Types.SMALLINT, "smallint", 1, 0);
    public static final JdbcTypeHandle JDBC_TINYINT = new JdbcTypeHandle(Types.TINYINT, "tinyint", 2, 0);
    public static final JdbcTypeHandle JDBC_INTEGER = new JdbcTypeHandle(Types.INTEGER, "integer", 4, 0);
    public static final JdbcTypeHandle JDBC_BIGINT = new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0);

    public static final JdbcTypeHandle JDBC_REAL = new JdbcTypeHandle(Types.REAL, "real", 8, 0);
    public static final JdbcTypeHandle JDBC_DOUBLE = new JdbcTypeHandle(Types.DOUBLE, "double precision", 8, 0);

    public static final JdbcTypeHandle JDBC_CHAR = new JdbcTypeHandle(Types.CHAR, "char", 10, 0);
    public static final JdbcTypeHandle JDBC_VARCHAR = new JdbcTypeHandle(Types.VARCHAR, "varchar", 10, 0);

    public static final JdbcTypeHandle JDBC_DATE = new JdbcTypeHandle(Types.DATE, "date", 8, 0);
    public static final JdbcTypeHandle JDBC_TIME = new JdbcTypeHandle(Types.TIME, "time", 4, 0);
    public static final JdbcTypeHandle JDBC_TIMESTAMP = new JdbcTypeHandle(Types.TIMESTAMP, "timestamp", 8, 0);
}
