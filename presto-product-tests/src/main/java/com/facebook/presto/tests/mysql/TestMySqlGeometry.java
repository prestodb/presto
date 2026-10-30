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
package com.facebook.presto.tests.mysql;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.MYSQL;
import static com.facebook.presto.tests.utils.QueryExecutors.onMySql;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestMySqlGeometry
        extends ProductTest
{
    @Test(groups = MYSQL)
    public void testReadGeometry()
    {
        String tableName = "test.jdbc_geometry_runtime_mapping";
        onMySql().executeQuery("CREATE TABLE " + tableName + " (shape GEOMETRY)");
        try {
            onMySql().executeQuery("INSERT INTO " + tableName + " VALUES " +
                    "(ST_GeomFromText('POINT(1 2)')), " +
                    "(ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'))");

            assertThat(query("SELECT CAST(shape AS VARCHAR) FROM mysql." + tableName))
                    .containsOnly(row("POINT (1 2)"), row("LINESTRING (0 0, 5 5, 10 10)"));
        }
        finally {
            onMySql().executeQuery("DROP TABLE " + tableName);
        }
    }
}
