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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.geospatial.type.GeometryType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestGeometryMapRead
        extends IcebergImportedTableTestBase
{
    private final String testName = "geometry_map";
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
    public void readGeomMapType()
    {
        // Assert schema creation
        String querySchema = format("SELECT 1 FROM %s.information_schema.schemata WHERE schema_name = '%s'", CATALOGNAME, SCHEMANAME);
        MaterializedResult resultSchema = computeActual(session, querySchema);
        assertEquals(resultSchema.getMaterializedRows().get(0).getField(0), 1);

        // Assert table creation
        String queryTable = format("SELECT 1 FROM %s.information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultTable = computeActual(session, queryTable);
        assertEquals(resultTable.getMaterializedRows().get(0).getField(0), 1);

        // Read geometry type for center
        String querySelect1 = format("SELECT mapgeometry['center'] FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect1 = computeActual(session, querySelect1);

        // Confirm geometry read
        assertEquals(resultSelect1.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect1.getMaterializedRows().get(0).getField(0), "POINT (-122.4194 37.7749)");

        // Read geometry type for boundary
        String querySelect2 = format("SELECT mapgeometry['boundary'] FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect2 = computeActual(session, querySelect2);

        // Confirm geometry read
        assertEquals(resultSelect2.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect2.getMaterializedRows().get(0).getField(0), "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        // Read geometry type for boundary
        String querySelect3 = format("SELECT ST_AsText(map_values(mapgeometry)[1]) FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect3 = computeActual(session, querySelect3);

        // Confirm geometry read
        assertEquals(resultSelect3.getTypes().get(0), VarcharType.VARCHAR);
        assertEquals(resultSelect3.getMaterializedRows().get(0).getField(0), "POINT (-122.4194 37.7749)");

        // Select geometries with Dimension 0
        String queryYMin = format("SELECT ST_YMin(map_values(mapgeometry)[1]) FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultYMin = computeActual(session, queryYMin);
        assertEquals(resultYMin.getMaterializedRows().get(0).getField(0), 37.7749);
    }
}
