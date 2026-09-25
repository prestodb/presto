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
import com.facebook.presto.common.type.TinyintType;
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

public class TestGeometryArrayRead
        extends IcebergImportedTableTestBase
{
    private final String testName = "geometry_array";
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
    public void readGeomArrayType()
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
        String querySelect1 = format("SELECT geom[1] FROM %s.%s.%s ORDER BY ST_AsText(geom[1]) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect1 = computeActual(session, querySelect1);

        // Confirm geometry read
        assertEquals(resultSelect1.getTypes().get(0), GeometryType.GEOMETRY);
        assertEquals(resultSelect1.getMaterializedRows().get(0).getField(0), "LINESTRING (0 0, 10 10, 20 20)");
        assertEquals(resultSelect1.getMaterializedRows().get(1).getField(0), "LINESTRING (0 0, 10 10, 20 20)");

        // Read geometry type
        String querySelect2 = format("SELECT ST_AsText(geom[2]) FROM %s.%s.%s ORDER BY ST_AsText(geom[2]) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect2 = computeActual(session, querySelect2);

        // Confirm geometry read
        assertEquals(resultSelect2.getTypes().get(0), VarcharType.VARCHAR);
        assertEquals(resultSelect2.getMaterializedRows().get(0).getField(0), "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))");
        assertEquals(resultSelect2.getMaterializedRows().get(1).getField(0), "POINT (10 20)");

        // Read geometry type
        String querySelect3 = format("SELECT ST_Dimension(geom[3]) FROM %s.%s.%s ORDER BY ST_AsText(geom[3]) ASC", CATALOGNAME, SCHEMANAME, testName);
        MaterializedResult resultSelect3 = computeActual(session, querySelect3);

        // Confirm geometry read
        assertEquals(resultSelect3.getTypes().get(0), TinyintType.TINYINT);
        assertEquals(resultSelect3.getMaterializedRows().get(0).getField(0).toString(), "0");
        assertEquals(resultSelect3.getMaterializedRows().get(1).getField(0).toString(), "2");
    }
}
