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
import java.util.Optional;

public final class TestingJdbcTypeHandle
{
    private TestingJdbcTypeHandle() {}

    public static final JdbcTypeHandle JDBC_BOOLEAN = new JdbcTypeHandle(Types.BOOLEAN, Optional.of("boolean"), 1, 0);

    public static final JdbcTypeHandle JDBC_SMALLINT = new JdbcTypeHandle(Types.SMALLINT, Optional.of("smallint"), 1, 0);
    public static final JdbcTypeHandle JDBC_TINYINT = new JdbcTypeHandle(Types.TINYINT, Optional.of("tinyint"), 2, 0);
    public static final JdbcTypeHandle JDBC_INTEGER = new JdbcTypeHandle(Types.INTEGER, Optional.of("integer"), 4, 0);
    public static final JdbcTypeHandle JDBC_BIGINT = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 8, 0);

    public static final JdbcTypeHandle JDBC_REAL = new JdbcTypeHandle(Types.REAL, Optional.of("real"), 8, 0);
    public static final JdbcTypeHandle JDBC_DOUBLE = new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), 8, 0);

    public static final JdbcTypeHandle JDBC_CHAR = new JdbcTypeHandle(Types.CHAR, Optional.of("char"), 10, 0);
    public static final JdbcTypeHandle JDBC_VARCHAR = new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), 10, 0);

    public static final JdbcTypeHandle JDBC_DATE = new JdbcTypeHandle(Types.DATE, Optional.of("date"), 8, 0);
    public static final JdbcTypeHandle JDBC_TIME = new JdbcTypeHandle(Types.TIME, Optional.of("time"), 4, 0);
    public static final JdbcTypeHandle JDBC_TIMESTAMP = new JdbcTypeHandle(Types.TIMESTAMP, Optional.of("timestamp"), 8, 0);
}
