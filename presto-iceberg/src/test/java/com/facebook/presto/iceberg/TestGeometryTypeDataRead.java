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

public class TestGeometryTypeDataRead
        extends IcebergImportedTableTestBase
{
    private final String testName = "geometry_data_type_read";
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
    public void readGeomDataType()
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
        String querySelect = format("SELECT * from %s.%s.%s ORDER BY ST_AsText(geom) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect = computeActual(session, querySelect);

        // Confirm geometry read
        assertEquals(resultSelect.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect.getMaterializedRows().get(0).getField(0), "LINESTRING (0 0, 10 10, 20 20)");
        assertEquals(resultSelect.getMaterializedRows().get(1).getField(0), "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))");
        assertEquals(resultSelect.getMaterializedRows().get(2).getField(0), "MULTIPOINT ((0 0), (10 20), (30 40))");
        assertEquals(resultSelect.getMaterializedRows().get(3).getField(0), "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((5 5, 9 5, 9 9, 5 9, 5 5)))");
        assertEquals(resultSelect.getMaterializedRows().get(4).getField(0), "POINT (10 20)");
        assertEquals(resultSelect.getMaterializedRows().get(5).getField(0), "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        // Select geometries with Dimension 0
        String queryDimension = format("SELECT * FROM %s.%s.%s WHERE ST_Dimension(geom)=0", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultDimension = computeActual(session, queryDimension);
        assertEquals(resultDimension.getMaterializedRows().size(), 2);

        // Select valid geometries
        String queryValid = format("SELECT * FROM %s.%s.%s WHERE ST_IsValid(geom)=true", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultValid = computeActual(session, queryValid);
        assertEquals(resultValid.getMaterializedRows().size(), 6);

        // Select geometries with XMax 10
        String queryXMax = format("SELECT * FROM %s.%s.%s WHERE ST_XMax(geom)=10", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultXMax = computeActual(session, queryXMax);
        assertEquals(resultXMax.getMaterializedRows().size(), 2);

        // Select geometries with YMax 20
        String queryYMax = format("SELECT * FROM %s.%s.%s WHERE ST_YMax(geom)=20", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultYMax = computeActual(session, queryYMax);
        assertEquals(resultYMax.getMaterializedRows().size(), 3);
    }
}
