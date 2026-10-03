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

public class TestGeometryRowRead
        extends IcebergImportedTableTestBase
{
    private final String testName = "geometry_row";
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
    public void readGeomRowType()
    {
        // Assert schema creation
        String querySchema = format("SELECT 1 FROM %s.information_schema.schemata WHERE schema_name = '%s'", CATALOGNAME, SCHEMANAME);
        MaterializedResult resultSchema = computeActual(session, querySchema);
        assertEquals(resultSchema.getMaterializedRows().get(0).getField(0), 1);

        // Assert table creation
        String queryTable = format("SELECT 1 FROM %s.information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultTable = computeActual(session, queryTable);
        assertEquals(resultTable.getMaterializedRows().get(0).getField(0), 1);

        // Read geometry type from geom1
        String querySelect1 = format("SELECT geometryrow.geom1 FROM %s.%s.%s ORDER BY ST_AsText(geometryrow.geom1) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect1 = computeActual(session, querySelect1);

        // Confirm geometry read
        assertEquals(resultSelect1.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect1.getMaterializedRows().get(0).getField(0), "POINT (1 2)");
        assertEquals(resultSelect1.getMaterializedRows().get(1).getField(0), "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");

        // Read geometry type from geom2
        String querySelect2 = format("SELECT geometryrow.geom2 FROM %s.%s.%s ORDER BY ST_AsText(geometryrow.geom2) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect2 = computeActual(session, querySelect2);

        // Confirm geometry read
        assertEquals(resultSelect2.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect2.getMaterializedRows().get(0).getField(0), "LINESTRING (0 0, 1 1, 2 2)");
        assertEquals(resultSelect2.getMaterializedRows().get(1).getField(0), "MULTIPOINT ((1 1), (2 2), (3 3))");

        // Select geometries with Dimension 0
        String queryDimension = format("SELECT ST_YMin(geometryrow.geom2) FROM %s.%s.%s ORDER BY ST_AsText(geometryrow.geom2) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultDimension = computeActual(session, queryDimension);
        assertEquals(resultDimension.getMaterializedRows().size(), 2);
    }
}
