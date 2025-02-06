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
package com.facebook.presto.rewriter;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.io.Resources;
import io.airlift.tpch.TpchTable;
import org.apache.commons.io.FileUtils;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.rewriter.util.ConfigConstants.IS_QUERY_REWRITER_PLUGIN_ENABLED;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.facebook.presto.tests.QueryAssertions.copyTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public abstract class AbstractOptPlusTestFramework
        extends AbstractTestQueryFramework
{
    public static final String PRESTO_USER = "presto_test";
    public static final String PRESTO_PASS = "test";
    public static final String ADMIN_ROLE = "admin";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");
    private static final String TEMPORARY_TABLE_SCHEMA = "__temporary_tables__";
    private static final String HIVE_CONNECTOR_NAME = "hive";
    private static final String TPCH_SOURCE_CATALOG = "tpchstandard";
    public static final String TRUSTSTORE_PASSWORD = "changeit";
    public static final String COORDINATOR_HOST = "localhost";

    private Map<String, String> optPlusConfig(int coordinatorPort)
    {
        return ImmutableMap.<String, String>builder()
                .put("optplus.jdbc.coordinator_host", COORDINATOR_HOST)
                .put("optplus.jdbc.coordinator_port", String.valueOf(coordinatorPort))
                .put("optplus.enable_fallback", String.valueOf(enableFallback()))
                .put("optplus.ssl.jdbc.enable", "true")
                .put("optplus.ssl.jdbc.trust_store_path", getResource("localhost.truststore").getPath())
                .put("optplus.ssl.jdbc.trust_store_password", TRUSTSTORE_PASSWORD)
                .put("optplus.db2.jdbc_url", getDb2JdbcUrl())
                .put("optplus.show-rewritten-query", "true")
                .put("optplus.username", PRESTO_USER)
                .put("optplus.password", PRESTO_PASS)
                .build();
    }

    private Session sessionWithRewritePluginEnabled;
    private Session sessionWithRewriterPluginDisabled;

    private String getDb2JdbcUrl()
    {
        return requireNonNull(System.getProperty("db2.jdbc_url"), "system property db2.jdbc_url is required");
    }

    protected String getDb2TpchSchema()
    {
        return requireNonNull(System.getProperty("db2.tpch_schema"), "system property db2.tpch_schema is required");
    }

    protected String getDb2TpchCatalog()
    {
        return requireNonNull(System.getProperty("db2.tpch_catalog"), "system property db2.tpch_catalog is required");
    }

    private Path getTestDataDir()
    {
        return Paths.get(requireNonNull(System.getProperty("test.data_dir"), "system property db2.tpch_catalog is required"));
    }

    protected boolean getTestGenerateResultFiles()
    {
        return Boolean.parseBoolean(requireNonNull(System.getProperty("test.generate_result_files"), "system property test.generate_result_files is required"));
    }

    protected boolean getTestEraseDataDir()
    {
        return Boolean.parseBoolean(requireNonNull(System.getProperty("test.erase_data_dir"), "system property test.erase_data_dir is required"));
    }

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return getQueryPaths()
                .collect(toDataProvider());
    }

    @Test(timeOut = 240_000, dataProvider = "getQueriesDataProvider")
    public void testQueries(String queryResourcePath)
    {
        if (!getTestGenerateResultFiles()) {
            testQuery(queryResourcePath);
        }
    }

    protected abstract boolean enableFallback();

    @AfterSuite
    protected void cleanUp()
            throws IOException
    {
        if (getTestEraseDataDir()) {
            FileUtils.deleteDirectory(getTestDataDir().toFile());
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // GovernanceUtil.setHiveCatalogSet(DB2_CATALOG);
        DistributedQueryRunner queryRunner = createQueryRunnerInternal(getSession());
        TestingPrestoServer coordinator = queryRunner.getCoordinator();
        queryRunner.installPlugin(new TestingRewriterPlugin());
        coordinator.getQueryRewriterManager().loadQueryRewriterProvider(optPlusConfig(coordinator.getHttpsAddress().getPort()));
        coordinator.getPasswordAuthenticatorManager().loadPasswordAuthenticator();
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return createQueryRunnerInternal(sessionWithRewriterPluginDisabled);
    }

    @Override
    protected Session getSession()
    {
        if (sessionWithRewritePluginEnabled == null) {
            SessionBuilder sessionBuilder = createSessionBuilder();
            sessionWithRewriterPluginDisabled = sessionBuilder.build();
            sessionBuilder.setSystemProperty(IS_QUERY_REWRITER_PLUGIN_ENABLED, "true");
            sessionWithRewritePluginEnabled = sessionBuilder.build();
        }
        return sessionWithRewritePluginEnabled;
    }

    private SessionBuilder createSessionBuilder()
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        PRESTO_USER,
                        Optional.empty(),
                        ImmutableMap.of(getDb2TpchCatalog(), new SelectedRole(ROLE, Optional.of(ADMIN_ROLE))),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .setCatalog(getDb2TpchCatalog())
                .setSchema(getDb2TpchSchema())
                .setClientTransactionSupport();
    }

    protected Stream<String> getQueryPaths()
    {
        return IntStream.rangeClosed(1, 22)
                .mapToObj(i -> format("q%02d", i))
                .map(queryId -> format("/tpch/%s.sql", queryId));
    }

    protected static String read(String resource)
    {
        try {
            return Resources.toString(getResource(TestOptPlusTPCHAgainstPrestoResults.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected String csvFromMaterializedResult(MaterializedResult result)
    {
        String header = "--types: " + Joiner.on("|")
                .join(result.getTypes().stream().map(x -> x.getTypeSignature().getBase().toUpperCase()).collect(toList()));
        String body = Joiner.on("\n")
                .join(result.getMaterializedRows().stream().map(x -> Joiner.on("|").join(x.getFields())).collect(toList()));
        return header + "\n" + body;
    }

    protected void generateResultsFiles(String result, String path)
            throws IOException
    {
        String absolutePath = getResource(TestOptPlusTPCHAgainstPrestoResults.class, path).getPath();
        BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(absolutePath));
        bufferedWriter.write(result);
        bufferedWriter.close();
    }

    private DistributedQueryRunner createQueryRunnerInternal(Session session)
            throws Exception
    {
        Map<String, String> systemProperties = ImmutableMap.<String, String>builder()
                .put("task.writer-count", "2")
                .put("task.partitioned-writer-count", "4")
                .put("tracing.tracer-type", "simple")
                .put("tracing.enable-distributed-tracing", "simple")
                .build();
        ImmutableMap<String, String> coordinatorProperties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.type", "PASSWORD")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.keystore.path", getResource("localhost.keystore").getPath())
                .put("http-server.https.keystore.key", TRUSTSTORE_PASSWORD)
                .put("http-server.https.truststore.path", getResource("localhost.truststore").getPath())
                .put("http-server.https.truststore.key", TRUSTSTORE_PASSWORD)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(systemProperties)
                .setDataDirectory(Optional.of(getTestDataDir()))
                .build();
        try {
            ExtendedHiveMetastore metastore = getFileHiveMetastore(queryRunner);
            queryRunner.installPlugin(new HivePlugin(HIVE_CONNECTOR_NAME, Optional.of(metastore)));
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.waitForClusterToGetReady();
            Map<String, String> tpchProperties = ImmutableMap.<String, String>builder()
                    .put("tpch.column-naming", "standard")
                    .build();
            queryRunner.createCatalog(TPCH_SOURCE_CATALOG, "tpch", tpchProperties);
            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .put("hive.time-zone", TIME_ZONE.getID())
                    .put("hive.security", "legacy")
                    .put("hive.allow-add-column", "true")
                    .put("hive.allow-drop-column", "true")
                    .put("hive.allow-drop-table", "true")
                    .put("hive.allow-rename-table", "true")
                    .put("hive.allow-rename-column", "true")
                    .put("hive.allow-drop-constraint", "true")
                    .put("hive.allow-add-constraint", "true")
                    .put("hive.max-partitions-per-scan", "1000")
                    .put("hive.assume-canonical-partition-keys", "true")
                    .put("hive.collect-column-statistics-on-write", "true")
                    .put("hive.temporary-table-schema", TEMPORARY_TABLE_SCHEMA)
                    .put("hive.storage-format", "TEXTFILE")
                    .put("hive.compression-codec", "NONE")
                    .build();
            queryRunner.createCatalog(getDb2TpchCatalog(), HIVE_CONNECTOR_NAME, hiveProperties);
            MetastoreContext metastoreContext = new MetastoreContext(PRESTO_USER, "test_queryId", Optional.empty(), ImmutableSet.of(),
                    Optional.empty(), Optional.empty(), false,
                    HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, WarningCollector.NOOP, new RuntimeStats());
            if (!metastore.getDatabase(metastoreContext, getDb2TpchSchema()).isPresent()) {
                metastore.createDatabase(metastoreContext, createDatabaseMetastoreObject(getDb2TpchSchema()));
                copyTables(queryRunner, "tpchstandard", TINY_SCHEMA_NAME, getSession(),
                        TpchTable.getTables().stream().map(TpchTable::getTableName).collect(toList()), true, false);
            }
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw new RuntimeException(e);
        }
    }

    private Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName(PRESTO_USER)
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    private ExtendedHiveMetastore getFileHiveMetastore(DistributedQueryRunner queryRunner)
    {
        File dataDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").toFile();
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration =
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, dataDirectory.toURI().toString(), PRESTO_USER);
    }

    protected void testQuery(String queryResourcePath)
    {
        @Language("SQL") String sql = read(queryResourcePath)
                .replaceAll("<schema>", getDb2TpchSchema().toLowerCase(Locale.getDefault()))
                .replaceAll("<catalog>", getDb2TpchCatalog());
        assertQueryInternal(sql);
    }

    protected void assertQueryInternal(String sql)
    {
        MaterializedResult actualResults = getQueryRunner().execute(sessionWithRewritePluginEnabled, sql).toTestTypes();
        MaterializedResult expectedResults = getExpectedQueryRunner().execute(sessionWithRewriterPluginDisabled, sql, actualResults.getTypes()).toTestTypes();
        assertTrue(sessionWithRewritePluginEnabled.getSystemProperty(IS_QUERY_REWRITER_PLUGIN_ENABLED, Boolean.class));
        assertEqualsTolerateError(actualResults, expectedResults, format("For query: %s", sql), 0.1);
    }

    private void assertEqualsTolerateError(Iterable<MaterializedRow> actual, Iterable<MaterializedRow> expected, String message, double errorToleranceFactor)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");
        ImmutableMultiset<MaterializedRow> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<MaterializedRow> expectedSet = ImmutableMultiset.copyOf(expected);
        if (actualSet.size() != expectedSet.size()) {
            printSetDifference(message, actualSet, expectedSet);
        }
        // Ideally we should sort the rows of actual and expected set, so that results match irrespective of the ordering.
        // Since, defining a comparator for MaterializedRow is non-trivial, will see need based.
        ImmutableList<MaterializedRow> actualRows = ImmutableList.copyOf(actualSet);
        ImmutableList<MaterializedRow> expectedRows = ImmutableList.copyOf(expectedSet);
        for (int i = 0; i < expectedRows.size(); i++) {
            MaterializedRow materializedExpectedRow = expectedRows.get(i);
            for (int j = 0; j < materializedExpectedRow.getFieldCount(); j++) {
                Object expectedValue = materializedExpectedRow.getField(j);
                Object actualValue = actualRows.get(i).getField(j);
                if (null == expectedValue || expectedValue.equals("null")) {
                    assertNull(actualValue, "null expected, but found not null value: " + actualValue);
                }
                else if (expectedValue instanceof BigDecimal) {
                    assertTrue(actualValue instanceof BigDecimal,
                            format("At column %d, %s Type of actual is %s is different from expected type BigDecimal", j, message, actualValue.getClass()));
                    Double expectedField = ((BigDecimal) expectedValue).doubleValue();
                    Double actualField = ((BigDecimal) actualValue).doubleValue();
                    if (Math.abs(expectedField - actualField) > errorToleranceFactor) {
                        printSetDifference(
                                format("%s At Row :%d Column :%d actual %g is different from expected %g", message, i, j, actualField, expectedField), actualSet, expectedSet);
                    }
                }
                else if (expectedValue instanceof Double) {
                    assertTrue(actualValue instanceof Double,
                            format("At column %d, %s Type of actual is %s is different from expected type Double", j, message, actualValue.getClass()));
                    Double expectedField = (Double) expectedValue;
                    Double actualField = (Double) actualValue;
                    if (Math.abs(expectedField - actualField) > errorToleranceFactor) {
                        printSetDifference(format("%s At Row :%d Column :%d actual %g is different from expected %g", message, i, j, actualField, expectedField), actualSet, expectedSet);
                    }
                }
                else if (expectedValue instanceof Float) {
                    assertTrue(actualValue instanceof Float, format("%s Type of actual is %s is different from expected type Float", message, actualValue.getClass()));
                    Float expectedField = (Float) expectedValue;
                    Float actualField = (Float) actualValue;
                    if (Math.abs(expectedField - actualField) > errorToleranceFactor) {
                        printSetDifference(format("%s At Row :%d Column :%d actual %g is different from expected %g", message, i, j, actualField, expectedField), actualSet, expectedSet);
                    }
                }
                else {
                    if (!Objects.deepEquals(expectedValue, actualValue)) {
                        printSetDifference(format("%s At Row :%d Column :%d actual %s is different from expected %s", message, i, j, actualValue, expectedValue), actualSet, expectedSet);
                    }
                }
            }
        }
    }

    private static void printSetDifference(String message, ImmutableMultiset<?> actualSet, ImmutableMultiset<?> expectedSet)
    {
        Multiset<?> unexpectedRows = Multisets.difference(actualSet, expectedSet);
        Multiset<?> missingRows = Multisets.difference(expectedSet, actualSet);
        int limit = 100;
        fail(format(
                "%snot equal\n" +
                        "Actual rows (up to %s of %s extra rows shown, %s rows in total):\n    %s\n" +
                        "Expected rows (up to %s of %s missing rows shown, %s rows in total):\n    %s\n",
                message == null ? "" : (message + "\n"),
                limit,
                unexpectedRows.size(),
                actualSet.size(),
                Joiner.on("\n    ").join(unexpectedRows.stream().limit(limit).collect(toList())),
                limit,
                missingRows.size(),
                expectedSet.size(),
                Joiner.on("\n    ").join(missingRows.stream().limit(limit).collect(toList()))));
    }
}
