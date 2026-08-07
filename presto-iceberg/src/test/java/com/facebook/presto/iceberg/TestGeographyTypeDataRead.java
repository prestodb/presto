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

import com.facebook.presto.Session;
import com.facebook.presto.geospatial.SphericalGeographyType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestGeographyTypeDataRead
        extends IcebergImportedTableTestBase
{
    private final String testName = "geography_data_type_read";
    private String tablePath;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        session = testSessionBuilder()
                .setCatalog(CATALOGNAME)
                .setSchema(SCHEMANAME)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setSchemaName(SCHEMANAME)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @BeforeMethod
    public void setup()
    {
        tablePath = setupAndRegisterTable(testName);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dropAndCleanupTable(testName, tablePath);
    }

    @Test
    public void readGeogDataType()
    {
        // Assert schema creation
        String querySchema = format("SELECT 1 FROM %s.information_schema.schemata WHERE schema_name = '%s'", CATALOGNAME, SCHEMANAME);
        MaterializedResult resultSchema = computeActual(session, querySchema);
        assertEquals(resultSchema.getMaterializedRows().get(0).getField(0), 1);

        // Assert table creation
        String queryTable = format("SELECT 1 FROM %s.information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultTable = computeActual(session, queryTable);
        assertEquals(resultTable.getMaterializedRows().get(0).getField(0), 1);

        // Read geometry type
        String querySelect = format("SELECT * FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect = computeActual(session, querySelect);

        // Confirm geometry read
        assertEquals(resultSelect.getTypes().get(2), SphericalGeographyType.SPHERICAL_GEOGRAPHY);
        assertEquals(resultSelect.getMaterializedRows().get(0).getField(2), "MULTIPOINT ((-122.4194 37.7749))");
        assertEquals(resultSelect.getMaterializedRows().get(1).getField(2), "MULTIPOINT ((-74.006 40.7128))");
        assertEquals(resultSelect.getMaterializedRows().get(2).getField(2), "MULTIPOINT ((-97.7431 30.2672))");
    }
}
