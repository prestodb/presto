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
package com.facebook.presto.tests.hive;

import com.google.inject.Inject;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_WITH_EXTERNAL_WRITES;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestHiveCreateExternalTable
        extends ProductTest
{
    private static final String HIVE_CATALOG_NAME = "hive_with_external_writes";

    @Inject
    private HdfsClient hdfsClient;

    @Test(groups = {HIVE_WITH_EXTERNAL_WRITES, PROFILE_SPECIFIC_TESTS})
    public void testCreateExternalTableWithInaccessibleSchemaLocation()
    {
        String schema = "schema_without_location";
        String schemaLocation = "/tmp/" + schema;
        hdfsClient.createDirectory(schemaLocation);
        query(format("CREATE SCHEMA %s.%s WITH (location='%s')",
                HIVE_CATALOG_NAME, schema, schemaLocation));

        hdfsClient.delete(schemaLocation);

        String table = "test_create_external";
        String tableLocation = "/tmp/" + table;
        query(format("CREATE TABLE %s.%s.%s WITH (external_location = '%s') AS " +
                        "SELECT * FROM tpch.tiny.nation",
                HIVE_CATALOG_NAME, schema, table, tableLocation));
    }
}
