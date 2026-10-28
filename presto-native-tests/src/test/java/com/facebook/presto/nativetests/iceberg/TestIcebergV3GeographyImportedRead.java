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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergImportedTableTestBase;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.StandardTypes.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativetests.iceberg.IcebergNullDomainAssertions.assertNullPredicateInScan;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Tests native reads of Iceberg format version 3 {@code geography} tables whose data files
 * were written by another engine rather than by Presto.
 *
 * <p>The data files under {@code iceberg_v3/geography_arrow_*} were written by PyArrow
 * (parquet-cpp-arrow 25.0.1), an implementation independent of the parquet-java writer
 * Presto uses, and wrapped in v3 table metadata by the Iceberg Java library. Each table has
 * columns {@code id INTEGER, source VARCHAR, geog geography, geogs array(geography)}: row
 * {@code i} holds the well-known text {@code source} of {@code geog} and a two element
 * {@code geogs} of that geography followed by a null, and the last row is all nulls.
 * <ul>
 *   <li>{@code geography_arrow_logical} annotates the geography columns with the Parquet
 *   {@code GEOGRAPHY} logical type (spherical edges, default CRS), as GeoArrow does.</li>
 *   <li>{@code geography_arrow_plain} stores them as unannotated {@code binary}, so only the
 *   Iceberg schema identifies them as geography.</li>
 * </ul>
 * Each read is checked against the geography built inline from {@code source} and compared
 * with the same query on the Java runner, which shares the native runner's metastore.
 *
 * <p>The remaining tables cover edge cases, and were wrapped in v3 table metadata the same way:
 * <ul>
 *   <li>{@code geography_arrow_null_file} has the {@code geography_arrow_logical} data file
 *   plus a second PyArrow file, rows {@code 10} to {@code 12}, in which every column but
 *   {@code id} is entirely null.</li>
 *   <li>{@code geography_arrow_crs} declares {@code geography(EPSG:4269)}, and its PyArrow
 *   file carries the same CRS in the Parquet {@code GEOGRAPHY} annotation.</li>
 *   <li>{@code geography_parquet_java_karney} declares {@code geography(OGC:CRS84, karney)},
 *   and its file, written by parquet-java because PyArrow writes only spherical edges,
 *   carries the {@code karney} algorithm in the Parquet {@code GEOGRAPHY} annotation.</li>
 *   <li>{@code geometry_arrow} has a {@code geometry} column, annotated with the Parquet
 *   {@code GEOMETRY} logical type, holding a point, a line string and a null.</li>
 * </ul>
 */
public class TestIcebergV3GeographyImportedRead
        extends IcebergImportedTableTestBase
{
    private static final String LOGICAL_TYPE_TABLE = "geography_arrow_logical";
    private static final String PLAIN_BINARY_TABLE = "geography_arrow_plain";
    private static final String NULL_FILE_TABLE = "geography_arrow_null_file";
    private static final String NON_WGS84_CRS_TABLE = "geography_arrow_crs";
    private static final String KARNEY_ALGORITHM_TABLE = "geography_parquet_java_karney";
    private static final String GEOMETRY_TABLE = "geometry_arrow";
    private static final int ROW_COUNT = 10;
    private static final int NULL_ROW_ID = 9;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        boolean sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @DataProvider(name = "tables")
    public static Object[][] tablesProvider()
    {
        return new Object[][] {
                {LOGICAL_TYPE_TABLE, true},
                {LOGICAL_TYPE_TABLE, false},
                {PLAIN_BINARY_TABLE, true},
                {PLAIN_BINARY_TABLE, false},
        };
    }

    @DataProvider(name = "pushdownFilterEnabled")
    public static Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @DataProvider(name = "unsupportedTables")
    public static Object[][] unsupportedTablesProvider()
    {
        return new Object[][] {
                {NON_WGS84_CRS_TABLE, "Iceberg geography type with CRS 'EPSG:4269' is not supported. Only WGS84 \\('OGC:CRS84' or 'EPSG:4326'\\) is supported"},
                {KARNEY_ALGORITHM_TABLE, "Iceberg geography type with edge interpolation algorithm 'karney' is not supported. Only 'spherical' is supported"},
        };
    }

    @Test(dataProvider = "tables")
    public void testReadGeography(String tableName, boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tablePath = setupAndRegisterTable(tableName);
        try {
            String table = format("%s.%s.%s", CATALOGNAME, SCHEMANAME, tableName);

            // The column reads back as a geography, not as the binary it is stored as.
            MaterializedResult result = computeActual(session, format("SELECT geog FROM %s WHERE id = 0", table));
            assertEquals(result.getTypes().get(0).getDisplayName(), SPHERICAL_GEOGRAPHY);
            assertEquals(result.getOnlyValue(), "POINT (-122.4194 37.7749)");

            // Every value, top level and nested, matches the geography built inline from its
            // source text. Compared within the native engine, so this also covers MULTIPOINT,
            // which the next query cannot.
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE geog IS NOT NULL " +
                            "AND ST_AsText(geog) = ST_AsText(to_spherical_geography(ST_GeometryFromText(source))) " +
                            "AND ST_AsText(geogs[1]) = ST_AsText(geog) AND geogs[2] IS NULL AND cardinality(geogs) = 2", table),
                    format("SELECT %d", ROW_COUNT - 1));
            assertQuery(
                    session,
                    format("SELECT id FROM %s WHERE geog IS NULL AND geogs IS NULL AND source IS NULL", table),
                    format("SELECT %d", NULL_ROW_ID));

            // Compared with the Java read of the same files. MULTIPOINT is left out because the
            // pinned Velox renders its well-known text differently from Java (fixed upstream by
            // facebookincubator/velox#18506), which is a function difference, not a read one.
            // TODO: Drop the MULTIPOINT exclusion once Velox is advanced past velox#18506.
            @Language("SQL") String everyKind = format(
                    "SELECT id, ST_AsText(geog), ST_AsText(geogs[1]), geogs[2] IS NULL, cardinality(geogs) FROM %s " +
                            "WHERE source IS NULL OR source NOT LIKE 'MULTIPOINT%%'",
                    table);
            assertQuery(session, everyKind, getSession(), everyKind);

            // Predicates on the geography column, evaluated in the scan when pushdown is enabled.
            // IS NOT NULL reaches the scan as an AllOrNoneValueSet domain, which the native
            // worker converts to a Velox IsNotNull filter. Spherical ST_Distance accepts only
            // points, and conjuncts may be evaluated in any order, so the other kinds are mapped
            // to null inside the call.
            assertNullPredicateInScan(
                    getQueryRunner(),
                    session,
                    format("SELECT count(*) FROM %s WHERE geog IS NOT NULL", table),
                    getExpectedQueryRunner(),
                    getSession(),
                    format("SELECT %d", ROW_COUNT - 1),
                    "geog",
                    false);
            assertQuery(
                    session,
                    format("SELECT id FROM %s WHERE ST_Distance(IF(ST_AsText(geog) LIKE 'POINT%%', geog), " +
                            "to_spherical_geography(ST_Point(-122.4194, 37.7749))) < 50000", table),
                    "SELECT 0");

            // Measurements accept the column with no cast and agree with the geography built
            // inline, within a tolerance.
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE id = 2 " +
                            "AND abs(ST_Area(geog) - ST_Area(to_spherical_geography(ST_GeometryFromText(source)))) < 1", table),
                    "SELECT 1");
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE id = 1 " +
                            "AND abs(ST_Length(geog) - ST_Length(to_spherical_geography(ST_GeometryFromText(source)))) < 1", table),
                    "SELECT 1");
        }
        finally {
            dropAndCleanupTable(tableName, tablePath);
        }
    }

    /**
     * A data file whose geography columns hold only nulls must read as nulls alongside a file
     * that holds values, and the null predicates must select across both files.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testReadDataFileWithOnlyNullGeography(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tablePath = setupAndRegisterTable(NULL_FILE_TABLE);
        try {
            String table = format("%s.%s.%s", CATALOGNAME, SCHEMANAME, NULL_FILE_TABLE);

            assertQuery(
                    session,
                    format("SELECT id, geog IS NULL, geogs IS NULL FROM %s WHERE id >= %d", table, NULL_ROW_ID),
                    format("VALUES (%d, true, true), (10, true, true), (11, true, true), (12, true, true)", NULL_ROW_ID));
            assertNullPredicateInScan(
                    getQueryRunner(),
                    session,
                    format("SELECT id FROM %s WHERE geog IS NULL", table),
                    getExpectedQueryRunner(),
                    getSession(),
                    format("VALUES %d, 10, 11, 12", NULL_ROW_ID),
                    "geog",
                    true);
            assertNullPredicateInScan(
                    getQueryRunner(),
                    session,
                    format("SELECT count(*) FROM %s WHERE geog IS NOT NULL", table),
                    getExpectedQueryRunner(),
                    getSession(),
                    format("SELECT %d", ROW_COUNT - 1),
                    "geog",
                    false);

            // MULTIPOINT is left out for the reason given in testReadGeography.
            // TODO: Drop the MULTIPOINT exclusion once Velox is advanced past velox#18506.
            @Language("SQL") String everyRow = format(
                    "SELECT id, ST_AsText(geog), ST_AsText(geogs[1]), cardinality(geogs) FROM %s " +
                            "WHERE source IS NULL OR source NOT LIKE 'MULTIPOINT%%'",
                    table);
            assertQuery(session, everyRow, getSession(), everyRow);
        }
        finally {
            dropAndCleanupTable(NULL_FILE_TABLE, tablePath);
        }
    }

    /**
     * Presto's SphericalGeography models only WGS84 coordinates with spherical edges, so a
     * column written by another engine with any other CRS or edge algorithm must be rejected
     * rather than read with the wrong semantics. The column type is checked on the
     * coordinator, before any split reaches the native worker.
     */
    @Test(dataProvider = "unsupportedTables")
    public void testUnsupportedGeographyIsRejected(String tableName, String expectedMessage)
    {
        String tablePath = setupAndRegisterTable(tableName);
        try {
            assertQueryFails(format("SELECT id, geog FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, tableName), expectedMessage);
        }
        finally {
            dropAndCleanupTable(tableName, tablePath);
        }
    }

    /**
     * Geometry is, like geography, neither comparable nor orderable, so its null predicates
     * also reach the scan as an AllOrNoneValueSet domain and go through the same conversion
     * to a Velox filter.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testGeometryNullPredicates(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tablePath = setupAndRegisterTable(GEOMETRY_TABLE);
        try {
            String table = format("%s.%s.%s", CATALOGNAME, SCHEMANAME, GEOMETRY_TABLE);

            assertNullPredicateInScan(
                    getQueryRunner(),
                    session,
                    format("SELECT id FROM %s WHERE geom IS NULL", table),
                    getExpectedQueryRunner(),
                    getSession(),
                    "VALUES 2",
                    "geom",
                    true);
            assertNullPredicateInScan(
                    getQueryRunner(),
                    session,
                    format("SELECT id, ST_AsText(geom) FROM %s WHERE geom IS NOT NULL", table),
                    getExpectedQueryRunner(),
                    getSession(),
                    "VALUES (0, 'POINT (-122.4194 37.7749)'), (1, 'LINESTRING (0 0, 10 10)')",
                    "geom",
                    false);
        }
        finally {
            dropAndCleanupTable(GEOMETRY_TABLE, tablePath);
        }
    }

    private Session sessionWithPushdown(boolean pushdownFilterEnabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
    }
}
