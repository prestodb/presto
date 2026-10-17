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
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Tests native reads of Iceberg format version 3 {@code geography} columns.
 *
 * <p>The native worker can read geography columns but not write them, so every table is
 * written through the Java query runner, which shares the native runner's warehouse, and
 * then read through the native one. Each read is compared with the same query on the Java
 * runner, which exercises the Java read path against the same files.
 */
public class TestIcebergV3Geography
        extends AbstractTestQueryFramework
{
    private static final String V3_PARQUET = "WITH (\"format-version\" = '3', format = 'PARQUET')";

    /**
     * One geography of every kind both engines accept, with valid longitude/latitude
     * coordinates.
     */
    private static final List<String> GEOGRAPHIES = ImmutableList.of(
            "POINT (-122.4194 37.7749)",
            "LINESTRING (0 0, 10 10, 20 20)",
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
            "MULTIPOINT ((0 0), (10 20), (30 40))",
            "MULTILINESTRING ((0 0, 5 5), (10 10, 20 20))",
            "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((5 5, 9 5, 9 9, 5 9, 5 5)))",
            "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
            "POINT EMPTY");

    protected boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
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

    @DataProvider(name = "pushdownFilterEnabled")
    public static Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testReadEveryGeographyKind(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String table = "geography_every_kind_" + pushdownFilterEnabled;
        try {
            // The source well-known text is stored next to each value, so a native query can
            // compare what it read with the same geography constructed inline.
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < GEOGRAPHIES.size(); i++) {
                values.append(format(
                        "(%d, '%s', to_spherical_geography(ST_GeometryFromText('%s'))), ",
                        i,
                        GEOGRAPHIES.get(i),
                        GEOGRAPHIES.get(i)));
            }
            values.append(format("(%d, NULL, NULL)", GEOGRAPHIES.size()));
            javaUpdate(format("CREATE TABLE %s (id INTEGER, source VARCHAR, geog SphericalGeography) %s", table, V3_PARQUET));
            javaUpdate(format("INSERT INTO %s VALUES %s", table, values));

            // Compared with the Java read of the same files. MULTIPOINT is left out because the
            // pinned Velox renders its well-known text differently from Java (fixed upstream by
            // facebookincubator/velox#18506), which is a function difference, not a read one;
            // the next query still covers it by comparing within the native engine.
            @Language("SQL") String everyKind = format("SELECT id, ST_AsText(geog) FROM %s WHERE source IS NULL OR source NOT LIKE 'MULTIPOINT%%'", table);
            assertQuery(session, everyKind, javaSession(), everyKind);
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE geog IS NOT NULL AND ST_AsText(geog) <> ST_AsText(to_spherical_geography(ST_GeometryFromText(source)))", table),
                    "SELECT 0");
            assertQuery(session, format("SELECT id FROM %s WHERE geog IS NULL", table), "SELECT 8");

            // The column reads back as a geography, not as the binary it is stored as.
            MaterializedResult result = computeActual(session, format("SELECT geog FROM %s WHERE id = 0", table));
            assertEquals(result.getTypes().get(0).getDisplayName(), SPHERICAL_GEOGRAPHY);
            assertEquals(result.getOnlyValue(), "POINT (-122.4194 37.7749)");

            // Measurements accept the column with no cast and agree with the geography built
            // inline. Compared with a tolerance natively, since the two engines' floating point
            // results need not match bit for bit.
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE ST_AsText(geog) LIKE 'POLYGON%%' " +
                            "AND abs(ST_Area(geog) - ST_Area(to_spherical_geography(ST_GeometryFromText(source)))) < 1", table),
                    "SELECT 1");
            assertQuery(
                    session,
                    format("SELECT count(*) FROM %s WHERE ST_AsText(geog) LIKE 'LINESTRING%%' " +
                            "AND abs(ST_Length(geog) - ST_Length(to_spherical_geography(ST_GeometryFromText(source)))) < 1", table),
                    "SELECT 1");
        }
        finally {
            dropTableIfExists(session, table);
        }
    }

    /**
     * With filter pushdown enabled, a predicate on the geography column is evaluated inside
     * the scan. It must see values already converted from the well-known binary on disk,
     * or a spherical function would receive bytes it cannot deserialize.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicateOnGeographyColumn(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String table = "geography_predicate_" + pushdownFilterEnabled;
        try {
            javaUpdate(format("CREATE TABLE %s (city VARCHAR, location SphericalGeography) %s", table, V3_PARQUET));
            javaUpdate(format(
                    "INSERT INTO %s VALUES " +
                            "('San Francisco', to_spherical_geography(ST_Point(-122.4194, 37.7749))), " +
                            "('Oakland', to_spherical_geography(ST_Point(-122.2711, 37.8044))), " +
                            "('Los Angeles', to_spherical_geography(ST_Point(-118.2437, 34.0522))), " +
                            "('Nowhere', NULL)",
                    table));

            // Within 50 km of San Francisco.
            assertQuery(
                    session,
                    format("SELECT city FROM %s WHERE ST_Distance(location, to_spherical_geography(ST_Point(-122.4194, 37.7749))) < 50000", table),
                    "VALUES 'San Francisco', 'Oakland'");
            assertQuery(session, format("SELECT city FROM %s WHERE location IS NULL", table), "VALUES 'Nowhere'");
            assertQuery(session, format("SELECT city FROM %s WHERE location IS NOT NULL AND city > 'M'", table), "VALUES 'Oakland', 'San Francisco'");
        }
        finally {
            dropTableIfExists(session, table);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testReadNestedGeography(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String table = "geography_nested_" + pushdownFilterEnabled;
        try {
            javaUpdate(format(
                    "CREATE TABLE %s (id INTEGER, geogs ARRAY(SphericalGeography), geog_row ROW(g SphericalGeography), geog_map MAP(VARCHAR, SphericalGeography)) %s",
                    table,
                    V3_PARQUET));
            javaUpdate(format(
                    "INSERT INTO %s SELECT 1, " +
                            "ARRAY[to_spherical_geography(ST_GeometryFromText('POINT (10 20)')), NULL], " +
                            "CAST(ROW(to_spherical_geography(ST_GeometryFromText('POINT (30 40)'))) AS ROW(g SphericalGeography)), " +
                            "MAP(ARRAY['a', 'b'], ARRAY[to_spherical_geography(ST_GeometryFromText('POINT (50 60)')), NULL])",
                    table));

            assertQuery(
                    session,
                    format("SELECT ST_AsText(geogs[1]), geogs[2] IS NULL, ST_AsText(geog_row.g), ST_AsText(geog_map['a']), geog_map['b'] IS NULL FROM %s", table),
                    "VALUES ('POINT (10 20)', true, 'POINT (30 40)', 'POINT (50 60)', true)");
            assertQuery(session, format("SELECT cardinality(geogs), cardinality(geog_map) FROM %s", table), "VALUES (2, 2)");
        }
        finally {
            dropTableIfExists(session, table);
        }
    }

    /**
     * The native writer does not yet convert Presto's geography serialization to the
     * well-known binary Iceberg requires, so a native write must fail rather than produce a
     * file no engine can read.
     */
    @Test
    public void testNativeWriteIsRejected()
    {
        String table = "geography_native_write";
        try {
            javaUpdate(format("CREATE TABLE %s (id INTEGER, geog SphericalGeography) %s", table, V3_PARQUET));
            assertQueryFails(
                    format("INSERT INTO %s VALUES (1, to_spherical_geography(ST_Point(10, 20)))", table),
                    ".*Writing an Iceberg geometry or geography column is not supported.*");
            assertQuery(format("SELECT count(*) FROM %s", table), "SELECT 0");
        }
        finally {
            dropTableIfExists(getSession(), table);
        }
    }

    /**
     * Runs a statement on the Java runner. The Java Iceberg connector rejects
     * {@code pushdown_filter_enabled}, so Java always runs with the default session.
     */
    private void javaUpdate(@Language("SQL") String sql)
    {
        getExpectedQueryRunner().execute(javaSession(), sql, ImmutableList.of(BIGINT));
    }

    private Session javaSession()
    {
        return getSession();
    }

    private Session sessionWithPushdown(boolean pushdownFilterEnabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
    }

    private void dropTableIfExists(Session session, String table)
    {
        assertUpdate(session, format("DROP TABLE IF EXISTS %s", table));
    }
}
