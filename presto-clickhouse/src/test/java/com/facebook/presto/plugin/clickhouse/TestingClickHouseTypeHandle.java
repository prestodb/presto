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
package com.facebook.presto.plugin.clickhouse;

import java.sql.Types;
import java.util.Optional;

public final class TestingClickHouseTypeHandle
{
    private TestingClickHouseTypeHandle() {}

    public static final ClickHouseTypeHandle JDBC_BOOLEAN = new ClickHouseTypeHandle(Types.BOOLEAN, Optional.of("boolean"), 1, 0, Optional.empty(), Optional.empty());

    public static final ClickHouseTypeHandle JDBC_SMALLINT = new ClickHouseTypeHandle(Types.SMALLINT, Optional.of("smallint"), 1, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_TINYINT = new ClickHouseTypeHandle(Types.TINYINT, Optional.of("tinyint"), 2, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_INTEGER = new ClickHouseTypeHandle(Types.INTEGER, Optional.of("integer"), 4, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_BIGINT = new ClickHouseTypeHandle(Types.BIGINT, Optional.of("bigint"), 8, 0, Optional.empty(), Optional.empty());

    public static final ClickHouseTypeHandle JDBC_REAL = new ClickHouseTypeHandle(Types.REAL, Optional.of("real"), 8, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_DOUBLE = new ClickHouseTypeHandle(Types.DOUBLE, Optional.of("double precision"), 8, 0, Optional.empty(), Optional.empty());

    public static final ClickHouseTypeHandle JDBC_CHAR = new ClickHouseTypeHandle(Types.CHAR, Optional.of("char"), 10, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_VARCHAR = new ClickHouseTypeHandle(Types.VARCHAR, Optional.of("varchar"), 10, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_STRING = new ClickHouseTypeHandle(Types.VARCHAR, Optional.of("String"), 10, 0, Optional.empty(), Optional.empty());

    public static final ClickHouseTypeHandle JDBC_DATE = new ClickHouseTypeHandle(Types.DATE, Optional.of("date"), 8, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_TIME = new ClickHouseTypeHandle(Types.TIME, Optional.of("time"), 4, 0, Optional.empty(), Optional.empty());
    public static final ClickHouseTypeHandle JDBC_TIMESTAMP = new ClickHouseTypeHandle(Types.TIMESTAMP, Optional.of("timestamp"), 8, 0, Optional.empty(), Optional.empty());
}
