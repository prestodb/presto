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

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.MetadataUtil.COLUMN_CODEC;
import static com.facebook.presto.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static com.facebook.presto.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestJdbcColumnHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(COLUMN_CODEC, new JdbcColumnHandle("connectorId", "columnName", JDBC_VARCHAR, VARCHAR));
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcColumnHandle("connectorId", "columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorId", "columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorId", "columnName", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("connectorId", "columnName", JDBC_VARCHAR, VARCHAR))
                .addEquivalentGroup(
                        new JdbcColumnHandle("connectorIdX", "columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorIdX", "columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorIdX", "columnName", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("connectorIdX", "columnName", JDBC_VARCHAR, VARCHAR))
                .addEquivalentGroup(
                        new JdbcColumnHandle("connectorId", "columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorId", "columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("connectorId", "columnNameX", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("connectorId", "columnNameX", JDBC_VARCHAR, VARCHAR))
                .check();
    }
}
