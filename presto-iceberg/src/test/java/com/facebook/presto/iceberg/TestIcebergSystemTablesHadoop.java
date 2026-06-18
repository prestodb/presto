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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;

public class TestIcebergSystemTablesHadoop
        extends TestIcebergSystemTables
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .build().getQueryRunner();
    }

    @Test
    public void testPropertiesTableWithSpecifiedDataWriteLocation()
            throws IOException
    {
        String dataLocation = Files.createTempDirectory("test_table_with_write_data_location").toAbsolutePath().toString();
        assertUpdate("CREATE SCHEMA test_schema_temp");
        try {
            assertUpdate(String.format("CREATE TABLE test_schema_temp.test_table_with_write_data_location (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'], \"write.data.path\" = '%s')", dataLocation));
            checkTableProperties("test_schema_temp", "test_table_with_write_data_location", "merge-on-read", dataLocation);
        }
        finally {
            assertUpdate("DROP TABLE test_schema_temp.test_table_with_write_data_location");
            assertUpdate("DROP SCHEMA test_schema_temp");
        }
    }
}
