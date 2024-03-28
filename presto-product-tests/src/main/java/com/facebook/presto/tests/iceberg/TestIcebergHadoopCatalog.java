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
package com.facebook.presto.tests.iceberg;

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.ICEBERG;
import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestIcebergHadoopCatalog
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        query("DROP TABLE IF EXISTS iceberg_hadoop.default.region");
        query("CREATE TABLE iceberg_hadoop.default.region AS SELECT * FROM tpch.tiny.region");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testTableSelect()
    {
        String regionQuery = "SELECT * FROM iceberg_hadoop.default.region";
        QueryResult result = query(regionQuery);
        assertThat(result).hasRowsCount(5);
    }

    @AfterTestWithContext
    public void tearDown()
    {
        query("DROP TABLE IF EXISTS iceberg_hadoop.default.region");
    }
}
