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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;

@Test
public class TestIcebergDistributedHive
        extends IcebergDistributedTestBase
{
    public TestIcebergDistributedHive()
    {
        super(HIVE, ImmutableMap.of("iceberg.hive-statistics-merge-strategy", "USE_NULLS_FRACTION_AND_NDV"));
    }

    @Override
    public void testNDVsAtSnapshot()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Override
    public void testStatsByDistance()
    {
        // ignore because HMS doesn't support statistics versioning
    }

    @Test
    public void testQuotedIdentifiers()
    {
        // Expected to fail as Table is stored in Uppercase in H2 db and exists in tpch as lowercase
        assertQueryFails("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"", "Table iceberg.tpch.ORDERS does not exist");
    }

    @Test
    public void testInformationSchemaUppercaseName()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'LOCAL'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TINY'",
                "SELECT 'tpch' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'",
                "SELECT '' WHERE false");
    }
}
