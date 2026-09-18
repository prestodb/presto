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
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

/**
 * Reads and writes Iceberg format-version 3 {@code geography} columns, which Iceberg stores
 * as well-known binary while Presto's SPHERICAL_GEOGRAPHY holds its own serialization.
 *
 * <p>Every table here is created and populated by Presto, so the tests cover both
 * directions of that conversion at once: a value that survives a write and a read is proof
 * that both conversions agree, and comparing it against the same geography written as a
 * literal is proof that they agree with the rest of the engine rather than merely with each
 * other.
 *
 * <p>Note that {@code ST_AsText} (JTS) and the rendering returned for the column itself
 * (ESRI) emit polygon rings in opposite orientations, so tests that need to identify a
 * specific row match on a geometry type prefix rather than on full text.
 */
@Test(singleThreaded = true)
public class TestGeographyType
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "geography";
    private static final String V3 = "WITH (format_version = '3')";

    /**
     * One geography of every type Presto's spherical geography functions accept, with valid
     * longitude/latitude coordinates. ESRI maps LINESTRING and MULTILINESTRING onto Polyline
     * and POLYGON and MULTIPOLYGON onto Polygon, so all six are supported.
     */
    private static final List<String> GEOGRAPHIES = ImmutableList.of(
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
     * Writes every supported geography type, plus a null, and reads them back. The source
     * well-known text is stored alongside each value so a single query can assert that no
     * row changed on the way through the file.
     */
    @Test
    public void testRoundTripOfEveryGeographyType()
    {
        String table = "test_geography_round_trip";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER, source VARCHAR, geog SphericalGeography) %s", table, V3));
        try {
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < GEOGRAPHIES.size(); i++) {
                values.append(format(
                        "(%d, '%s', to_spherical_geography(ST_GeometryFromText('%s'))), ",
                        i,
                        GEOGRAPHIES.get(i),
                        GEOGRAPHIES.get(i)));
            }
            values.append(format("(%d, NULL, NULL)", GEOGRAPHIES.size()));
            assertUpdate(session, format("INSERT INTO %s VALUES %s", table, values), GEOGRAPHIES.size() + 1);

            // Every stored value must render exactly as the same geography written inline,
            // which holds only if the write and read conversions are mutual inverses
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE geog IS NOT NULL AND ST_AsText(geog) != ST_AsText(to_spherical_geography(ST_GeometryFromText(source)))", table),
                    "SELECT 0");

            // The column reads back as a geography, not as the binary it is stored as
            MaterializedResult result = computeActual(session, format("SELECT geog FROM %s ORDER BY id", table));
            assertEquals(result.getTypes().get(0), SPHERICAL_GEOGRAPHY);
            assertEquals(result.getMaterializedRows().size(), GEOGRAPHIES.size() + 1);
            assertEquals(result.getMaterializedRows().get(0).getField(0), "POINT (10 20)");
            assertEquals(result.getMaterializedRows().get(1).getField(0), "LINESTRING (0 0, 10 10, 20 20)");
            assertEquals(result.getMaterializedRows().get(3).getField(0), "MULTIPOINT ((0 0), (10 20), (30 40))");
            assertEquals(result.getMaterializedRows().get(4).getField(0), "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))");
            // The null must survive both conversions
            assertEquals(result.getMaterializedRows().get(GEOGRAPHIES.size()).getField(0), null);

            // Measurement functions must accept the column with no cast, and agree with the
            // same geography written as a literal
            MaterializedResult areas = computeActual(session, format(
                    "SELECT ST_Area(geog), ST_Area(to_spherical_geography(ST_GeometryFromText(source))) FROM %s WHERE ST_AsText(geog) LIKE 'POLYGON%%'",
                    table));
            assertEquals(areas.getMaterializedRows().size(), 1);
            assertEquals(
                    (double) areas.getMaterializedRows().get(0).getField(0),
                    (double) areas.getMaterializedRows().get(0).getField(1),
                    1.0);

            MaterializedResult distance = computeActual(session, format(
                    "SELECT ST_Distance(geog, to_spherical_geography(ST_GeometryFromText(source))) FROM %s WHERE ST_AsText(geog) LIKE 'POINT%%'",
                    table));
            assertEquals(distance.getMaterializedRows().size(), 1);
            assertEquals((double) distance.getMaterializedRows().get(0).getField(0), 0.0, 1.0E-9);

            // The declared type must survive as geography rather than decay to binary, both
            // in the engine and in the Iceberg schema on disk
            assertEquals(
                    computeActual(session, format(
                            "SELECT data_type FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = 'geog'",
                            SCHEMA,
                            table)).getMaterializedRows().get(0).getField(0),
                    SPHERICAL_GEOGRAPHY.getDisplayName());
            assertThat(tableMetadataJson(table)).contains("\"name\":\"geog\",\"required\":false,\"type\":\"geography\"");
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
        }
    }

    @Test
    public void testRoundTripOfNestedGeography()
    {
        String table = "test_nested_geography_round_trip";
        assertUpdate(session, format(
                "CREATE TABLE %s (id INTEGER, geogs ARRAY(SphericalGeography), geog_row ROW(g SphericalGeography), geog_map MAP(VARCHAR, SphericalGeography)) %s",
                table,
                V3));
        try {
            assertUpdate(session, format(
                    "INSERT INTO %s SELECT 1, " +
                            "ARRAY[to_spherical_geography(ST_GeometryFromText('POINT (10 20)')), NULL], " +
                            "CAST(ROW(to_spherical_geography(ST_GeometryFromText('POINT (30 40)'))) AS ROW(g SphericalGeography)), " +
                            "MAP(ARRAY['a'], ARRAY[to_spherical_geography(ST_GeometryFromText('POINT (50 60)'))])",
                    table), 1);

            MaterializedResult result = computeActual(session, format(
                    "SELECT ST_AsText(geogs[1]), geogs[2], ST_AsText(geog_row.g), ST_AsText(geog_map['a']) FROM %s",
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
     * The written Parquet file must carry the GEOGRAPHY logical annotation, which is what
     * makes the column recognizable to a reader that does not consult the Iceberg schema.
     * The Iceberg library cannot emit it, so this asserts the replacement converter ran.
     */
    @Test
    public void testWrittenParquetFileCarriesGeographyAnnotation()
            throws Exception
    {
        String table = "test_geography_annotation";
        assertUpdate(session, format("CREATE TABLE %s (geog SphericalGeography) %s", table, V3));
        try {
            assertUpdate(session, format("INSERT INTO %s VALUES to_spherical_geography(ST_GeometryFromText('POINT (10 20)'))", table), 1);

            List<Path> dataFiles = dataFiles(table);
            assertEquals(dataFiles.size(), 1);
            PrimitiveType column = parquetColumn(dataFiles.get(0), "geog");
            assertEquals(column.getPrimitiveTypeName(), BINARY);
            // The parameterless annotation is the default CRS and algorithm, matching the
            // canonical "geography" the Iceberg schema records
            assertEquals(column.getLogicalTypeAnnotation(), LogicalTypeAnnotation.geographyType());
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
        String table = "test_geography_bounds";
        assertUpdate(session, format("CREATE TABLE %s (id INTEGER, geog SphericalGeography) %s", table, V3));
        try {
            assertUpdate(session, format(
                    "INSERT INTO %s VALUES (1, to_spherical_geography(ST_GeometryFromText('POINT (10 20)')))",
                    table), 1);

            MaterializedResult files = computeActual(session, format("SELECT lower_bounds, upper_bounds FROM \"%s$files\"", table));
            assertEquals(files.getMaterializedRows().size(), 1);
            // Field 1 is the id column and keeps its bounds; field 2 is the geography column
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

    private String tableMetadataJson(String table)
    {
        try {
            Path metadataDirectory = tableDirectory(table).resolve("metadata");
            try (Stream<Path> files = Files.list(metadataDirectory)) {
                Path latest = files.filter(path -> path.toString().endsWith(".metadata.json"))
                        .max(Comparator.comparing(Path::toString))
                        .orElseThrow(() -> new IllegalStateException("no metadata file for " + table));
                return new String(Files.readAllBytes(latest), UTF_8);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
