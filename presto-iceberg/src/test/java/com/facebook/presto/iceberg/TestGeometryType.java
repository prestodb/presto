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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

/**
 * Reads and writes Iceberg format-version 3 {@code geometry} columns, which Iceberg stores
 * as well-known binary while Presto's GEOMETRY holds its own serialization.
 *
 * <p>Every table here is created and populated by Presto, so the tests cover both
 * directions of that conversion at once: a value that survives a write and a read is proof
 * that both conversions agree, and comparing it against the same geometry written as a
 * literal is proof that they agree with the rest of the engine rather than merely with each
 * other.
 */
@Test(singleThreaded = true)
public class TestGeometryType
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "geometry";
    private static final String V2 = "WITH (\"format-version\" = '2')";
    private static final String V3 = "WITH (\"format-version\" = '3')";
    private static final String V3_ORC = "WITH (\"format-version\" = '3', \"write.format.default\" = 'ORC')";

    /**
     * One geometry of every type Presto functions accept.
     */
    private static final List<String> GEOMETRIES = ImmutableList.of(
            "POINT (10 20)",
            "LINESTRING (0 0, 10 10, 20 20)",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "MULTIPOINT ((0 0), (10 20), (30 40))",
            "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))",
            "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((5 5, 9 5, 9 9, 5 9, 5 5)))");

    private Session session;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema(SCHEMA)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setSchemaName(SCHEMA)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    /**
     * Writes every supported geometry type, plus a null, and reads them back. The source
     * well-known text is stored alongside each value so a single query can assert that no
     * row changed on the way through the file.
     */
    @Test
    public void testRoundTripOfEveryGeometryType()
    {
        String table = "test_geometry_round_trip";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER, source VARCHAR, geom Geometry) %s", table, V3));
        try {
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < GEOMETRIES.size(); i++) {
                values.append(format(
                        "(%d, '%s', ST_GeometryFromText('%s')), ",
                        i,
                        GEOMETRIES.get(i),
                        GEOMETRIES.get(i)));
            }
            values.append(format("(%d, NULL, NULL)", GEOMETRIES.size()));
            assertUpdate(session, format("INSERT INTO %s VALUES %s", table, values), GEOMETRIES.size() + 1);

            // Every stored value must render exactly as the same geometry written inline,
            // which holds only if the write and read conversions are mutual inverses
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE geom IS NOT NULL AND ST_AsText(geom) != ST_AsText(ST_GeometryFromText(source))", table),
                    "SELECT 0");

            // The column reads back as a geometry, not as the binary it is stored as
            MaterializedResult result = computeActual(session, format("SELECT geom FROM %s ORDER BY id", table));
            assertEquals(result.getTypes().get(0), GEOMETRY);
            assertEquals(result.getMaterializedRows().size(), GEOMETRIES.size() + 1);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "POINT (10 20)");
            assertEquals(result.getMaterializedRows().get(1).getField(0), "LINESTRING (0 0, 10 10, 20 20)");
            assertEquals(result.getMaterializedRows().get(3).getField(0), "MULTIPOINT ((0 0), (10 20), (30 40))");
            assertEquals(result.getMaterializedRows().get(4).getField(0), "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))");
            // The null must survive both conversions
            assertEquals(result.getMaterializedRows().get(GEOMETRIES.size()).getField(0), null);

            // Measurement functions must accept the column with no cast, and agree with the
            // same geometry written as a literal
            MaterializedResult areas = computeActual(session, format(
                    "SELECT ST_Area(geom), ST_Area(ST_GeometryFromText(source)) FROM %s WHERE ST_AsText(geom) LIKE 'POLYGON%%'",
                    table));
            assertEquals(areas.getMaterializedRows().size(), 1);
            assertEquals(
                    (double) areas.getMaterializedRows().get(0).getField(0),
                    (double) areas.getMaterializedRows().get(0).getField(1),
                    1.0);

            MaterializedResult distance = computeActual(session, format(
                    "SELECT ST_Distance(geom, ST_GeometryFromText(source)) FROM %s WHERE ST_AsText(geom) LIKE 'POINT%%'",
                    table));
            assertEquals(distance.getMaterializedRows().size(), 1);
            assertEquals((double) distance.getMaterializedRows().get(0).getField(0), 0.0, 1.0E-9);

            // The declared type must survive as geometry rather than decay to binary, both
            // in the engine and in the Iceberg schema on disk
            assertEquals(
                    computeActual(session, format(
                            "SELECT data_type FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'geom'",
                            SCHEMA,
                            table)).getMaterializedRows().get(0).getField(0),
                    GEOMETRY.getDisplayName());
            // Presto's defaults leave the CRS and algorithm unset, the canonical "geometry"
            assertEquals(loadTable(table).schema().findType("geom"), Types.GeometryType.crs84());
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    @Test
    public void testRoundTripOfNestedGeometry()
    {
        String table = "test_nested_geometry_round_trip";
        assertUpdate(session, format(
                "CREATE TABLE %s (id INTEGER, geoms ARRAY(Geometry), geom_row ROW(g Geometry), geom_map MAP(VARCHAR, Geometry)) %s",
                table,
                V3));
        try {
            assertUpdate(session, format(
                    "INSERT INTO %s SELECT 1, " +
                            "ARRAY[ST_GeometryFromText('POINT (10 20)'), NULL], " +
                            "CAST(ROW(ST_GeometryFromText('POINT (30 40)')) AS ROW(g Geometry)), " +
                            "MAP(ARRAY['a'], ARRAY[ST_GeometryFromText('POINT (50 60)')])",
                    table), 1);

            MaterializedResult result = computeActual(session, format(
                    "SELECT ST_AsText(geoms[1]), geoms[2], ST_AsText(geom_row.g), ST_AsText(geom_map['a']) FROM %s",
                    table));
            assertEquals(result.getMaterializedRows().get(0).getField(0), "POINT (10 20)");
            assertEquals(result.getMaterializedRows().get(0).getField(1), null);
            assertEquals(result.getMaterializedRows().get(0).getField(2), "POINT (30 40)");
            assertEquals(result.getMaterializedRows().get(0).getField(3), "POINT (50 60)");
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    /**
     * The written Parquet file must carry the GEOMETRY logical annotation, which is what
     * makes the column recognizable to a reader that does not consult the Iceberg schema.
     * The Iceberg library cannot emit it, so this asserts the replacement converter ran.
     */
    @Test
    public void testWrittenParquetFileCarriesGeometryAnnotation()
            throws Exception
    {
        String table = "test_geometry_annotation";
        assertUpdate(session, format("CREATE TABLE %s (geom Geometry) %s", table, V3));
        try {
            assertUpdate(session, format("INSERT INTO %s VALUES ST_GeometryFromText('POINT (10 20)')", table), 1);

            List<Path> dataFiles = dataFiles(table);
            assertEquals(dataFiles.size(), 1);
            PrimitiveType column = parquetColumn(dataFiles.get(0), "geom");
            assertEquals(column.getPrimitiveTypeName(), BINARY);
            // The parameterless annotation is the default CRS and algorithm, matching the
            // canonical "geometry" the Iceberg schema records
            assertEquals(column.getLogicalTypeAnnotation(), LogicalTypeAnnotation.geometryType(LogicalTypeAnnotation.DEFAULT_CRS));
            assertEquals(column.getId().intValue(), 1);
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    /**
     * Iceberg computes a geospatial column's bounds as if it were binary, producing a byte
     * comparison of well-known binary where the specification calls for geospatial bounds.
     * Such bounds must not be recorded: a reader following the specification could prune on
     * them and silently drop rows, and Iceberg cannot even deserialize them, which would
     * break reading {@code $files} and collecting statistics.
     */
    @Test
    public void testGeospatialBoundsAreNotRecorded()
    {
        String table = "test_geometry_bounds";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER, geom Geometry) %s", table, V3));
        try {
            assertUpdate(session, format(
                    "INSERT INTO %s VALUES (1, ST_GeometryFromText('POINT (10 20)'))",
                    table), 1);

            MaterializedResult files = computeActual(session, format("SELECT lower_bounds, upper_bounds FROM \"%s$files\"", table));
            assertEquals(files.getMaterializedRows().size(), 1);
            // Field 1 is the id column and keeps its bounds; field 2 is the geometry column
            Map<Object, Object> lowerBounds = (Map<Object, Object>) files.getMaterializedRows().get(0).getField(0);
            Map<Object, Object> upperBounds = (Map<Object, Object>) files.getMaterializedRows().get(0).getField(1);
            assertThat(lowerBounds).containsKey(1);
            assertThat(lowerBounds).doesNotContainKey(2);
            assertThat(upperBounds).containsKey(1);
            assertThat(upperBounds).doesNotContainKey(2);
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    /**
     * Iceberg allows geometry columns only from format version 3, and Presto writes them
     * only to Parquet, so both must be rejected when the table is created rather than
     * when the first data file is written.
     */
    @Test
    public void testCreateTableRejectsUnsupportedGeometryTable()
    {
        assertQueryFails(
                session,
                "CREATE TABLE test_geometry_v2 (geom Geometry) " + V2,
                "Iceberg geometry column 'geom' requires format version 3 or higher, but the table is at format version 2");
        assertQueryFails(
                session,
                "CREATE TABLE test_geometry_v2 AS SELECT ST_GeometryFromText('POINT (10 20)') geom",
                "Iceberg geometry column 'geom' requires format version 3 or higher, but the table is at format version 2");
        assertQueryFails(
                session,
                "CREATE TABLE test_geometry_v2 (geom Geometry) " + V3_ORC,
                "Writing to Iceberg geometry column 'geom' is only supported for the PARQUET file format, but the table uses ORC");
    }

    /**
     * ADD COLUMN must apply the same checks as CREATE TABLE, so that the schema is never
     * changed to one that no later write could satisfy.
     */
    @Test
    public void testAddColumnRejectsUnsupportedGeometryColumn()
    {
        String v2Table = "test_geometry_add_column_v2";
        String orcTable = "test_geometry_add_column_orc";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER) %s", v2Table, V2));
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER) %s", orcTable, V3_ORC));
        try {
            assertQueryFails(
                    session,
                    format("ALTER TABLE %s ADD COLUMN geom Geometry", v2Table),
                    "Iceberg geometry column 'geom' requires format version 3 or higher, but the table is at format version 2");
            assertQueryFails(
                    session,
                    format("ALTER TABLE %s ADD COLUMN geoms ARRAY(Geometry)", v2Table),
                    "Iceberg geometry column 'geoms' requires format version 3 or higher, but the table is at format version 2");
            assertQueryFails(
                    session,
                    format("ALTER TABLE %s ADD COLUMN geom Geometry", orcTable),
                    "Writing to Iceberg geometry column 'geom' is only supported for the PARQUET file format, but the table uses ORC");

            // The rejected columns must not have reached the schema
            assertQuery(session, format("SELECT count(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA, v2Table), "SELECT 1");
            assertQuery(session, format("SELECT count(*) FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", SCHEMA, orcTable), "SELECT 1");
        }
        finally {
            assertUpdate(session, "DROP TABLE " + v2Table);
            assertUpdate(session, "DROP TABLE " + orcTable);
        }
    }

    @Test
    public void testAddGeometryColumn()
    {
        String table = "test_geometry_add_column";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER) %s", table, V3));
        try {
            assertUpdate(session, format("INSERT INTO %s VALUES 1", table), 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN geom Geometry", table));
            assertUpdate(session, format("INSERT INTO %s VALUES (2, ST_GeometryFromText('POINT (10 20)'))", table), 1);

            assertQuery(session, format("SELECT id, ST_AsText(geom) FROM %s", table), "VALUES (1, NULL), (2, 'POINT (10 20)')");
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    /**
     * EPSG:4326 names the same WGS84 longitude-latitude coordinates as the default CRS, so a
     * column created by another engine with that CRS can be written as well as read, and the
     * write must leave the declared CRS in place.
     */
    @Test
    public void testWriteToEpsg4326GeometryColumn()
    {
        String table = "test_geometry_epsg_4326";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER) %s", table, V3));
        try {
            assertUpdate(session, format("INSERT INTO %s VALUES 1", table), 1);
            // Presto always creates geometry columns with the default CRS, so the column is
            // added the way another engine would add it
            loadTable(table).updateSchema().addColumn("geom", Types.GeometryType.of("EPSG:4326")).commit();

            assertUpdate(session, format("INSERT INTO %s VALUES (2, ST_GeometryFromText('POINT (10 20)'))", table), 1);

            assertQuery(session, format("SELECT id, ST_AsText(geom) FROM %s", table), "VALUES (1, NULL), (2, 'POINT (10 20)')");
            assertEquals(loadTable(table).schema().findType("geom"), Types.GeometryType.of("EPSG:4326"));
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    private Table loadTable(String table)
    {
        return new HadoopTables(new Configuration()).load(tableDirectory(table).toString());
    }

    private List<Path> dataFiles(String table)
            throws Exception
    {
        try (Stream<Path> files = Files.walk(tableDirectory(table).resolve("data"))) {
            return files.filter(path -> path.toString().endsWith(".parquet"))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    private Path tableDirectory(String table)
    {
        String path = (String) computeActual(session, format("SELECT \"$path\" FROM %s LIMIT 1", table))
                .getMaterializedRows().get(0).getField(0);
        // .../<table>/data/<file>.parquet
        return new File(path.replaceFirst("^file:", "")).getParentFile().getParentFile().toPath();
    }

    private static PrimitiveType parquetColumn(Path file, String name)
            throws Exception
    {
        Configuration configuration = new Configuration();
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toString()), configuration),
                HadoopReadOptions.builder(configuration).build())) {
            return reader.getFileMetaData().getSchema().getType(name).asPrimitiveType();
        }
    }
}
