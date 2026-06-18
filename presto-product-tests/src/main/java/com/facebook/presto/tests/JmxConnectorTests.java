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
package com.facebook.presto.tests;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.JMX_CONNECTOR;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.VARCHAR;

public class JmxConnectorTests
        extends ProductTest
{
    @Test(groups = {JMX_CONNECTOR, JDBC})
    public void selectFromJavaRuntimeJmxMBean()
    {
        String sql = "SELECT node, vmname, vmversion FROM jmx.current.\"java.lang:type=runtime\"";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = JMX_CONNECTOR)
    public void selectFromJavaOperatingSystemJmxMBean()
    {
        assertThat(query("SELECT openfiledescriptorcount, maxfiledescriptorcount " +
                "FROM jmx.current.\"java.lang:type=operatingsystem\""))
                .hasColumns(BIGINT, BIGINT)
                .hasAnyRows();
    }
}
