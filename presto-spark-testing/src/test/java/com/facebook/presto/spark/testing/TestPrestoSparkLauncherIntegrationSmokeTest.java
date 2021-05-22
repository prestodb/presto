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
package com.facebook.presto.spark.testing;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.hive.HiveHadoop2Plugin;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import io.airlift.units.Duration;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.facebook.presto.spark.testing.Processes.destroyProcess;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.asByteSource;
import static com.google.common.io.Files.write;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.net.NetUtils.addStaticResolution;
import static org.testng.Assert.assertEquals;

/**
 * IMPORTANT!
 * <p>
 * Before running this test from an IDE, the project must be built with maven.
 * <p>
 * Please run:
 * <p>
 * ./mvnw clean install -pl presto-spark-launcher,presto-spark-package -am -DskipTests
 * <p>
 * from the project root after making any changes to the presto-spark-* codebase,
 * otherwise this test may be running an old code version
 */
@Test(singleThreaded = true)
public class TestPrestoSparkLauncherIntegrationSmokeTest
{
    private static final Logger log = Logger.get(TestPrestoSparkLauncherIntegrationSmokeTest.class);
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    private File tempDir;
    private File sparkWorkDirectory;

    private DockerCompose dockerCompose;
    private Process composeProcess;
    private LocalQueryRunner localQueryRunner;

    private File prestoLauncher;
    private File prestoPackage;

    private File configProperties;
    private File catalogDirectory;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
        // the default temporary directory location on MacOS is not sharable to docker
        tempDir = new File("/tmp", randomUUID().toString());
        createDirectories(tempDir.toPath());
        sparkWorkDirectory = new File(tempDir, "work");
        createDirectories(sparkWorkDirectory.toPath());

        File composeYaml = extractResource("docker-compose.yml", tempDir);
        dockerCompose = new DockerCompose(composeYaml);
        dockerCompose.verifyInstallation();
        dockerCompose.pull();
        composeProcess = dockerCompose.up(ImmutableMap.of(
                "spark-master", 1,
                "spark-worker", 2,
                "hadoop-master", 1));

        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("default")
                .build();
        localQueryRunner = new LocalQueryRunner(session);
        HiveHadoop2Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory hiveConnectorFactory = getOnlyElement(plugin.getConnectorFactories());
        addStaticResolution("hadoop-master", "127.0.0.1");
        String hadoopMasterAddress = dockerCompose.getContainerAddress("hadoop-master");
        // datanode is accessed via the internal docker IP address that is not accessible from the host
        addStaticResolution(hadoopMasterAddress, "127.0.0.1");
        localQueryRunner.createCatalog(
                "hive",
                hiveConnectorFactory,
                ImmutableMap.of(
                        "hive.metastore.uri", "thrift://127.0.0.1:9083",
                        "hive.time-zone", TIME_ZONE.getID(),
                        "hive.experimental-optimized-partition-update-serialization-enabled", "true"));
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(), ImmutableMap.of());
        // it may take some time for the docker container to start
        ensureHiveIsRunning(localQueryRunner, new Duration(10, MINUTES));
        importTables(localQueryRunner, "lineitem", "orders");
        importTablesBucketed(localQueryRunner, ImmutableList.of("orderkey"), "lineitem", "orders");

        File projectRoot = resolveProjectRoot();
        prestoLauncher = resolveFile(new File(projectRoot, "presto-spark-launcher/target"), Pattern.compile("presto-spark-launcher-[\\d\\.]+(-SNAPSHOT)?\\.jar"));
        logPackageInfo(prestoLauncher);
        prestoPackage = resolveFile(new File(projectRoot, "presto-spark-package/target"), Pattern.compile("presto-spark-package-.+\\.tar\\.gz"));
        logPackageInfo(prestoPackage);

        configProperties = new File(tempDir, "config.properties");
        storeProperties(configProperties, ImmutableMap.of(
                "query.hash-partition-count", "10"));
        catalogDirectory = new File(tempDir, "catalogs");
        createDirectories(catalogDirectory.toPath());
        storeProperties(new File(catalogDirectory, "hive.properties"), ImmutableMap.of(
                "connector.name", "hive-hadoop2",
                "hive.metastore.uri", "thrift://hadoop-master:9083",
                // hadoop native cannot be run within the spark docker container
                // the getnetgrent dependency is missing
                "hive.dfs.require-hadoop-native", "false",
                "hive.time-zone", TIME_ZONE.getID()));
        storeProperties(new File(catalogDirectory, "tpch.properties"), ImmutableMap.of(
                "connector.name", "tpch",
                "tpch.splits-per-node", "4",
                "tpch.partitioning-enabled", "false"));
    }

    private static void ensureHiveIsRunning(LocalQueryRunner localQueryRunner, Duration timeout)
            throws InterruptedException, TimeoutException
    {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (tryCreateDummyTable(localQueryRunner)) {
                return;
            }
            sleep(1000);
        }
        throw new TimeoutException(format("Not able to create a dummy table in hive after %s, most likely the hive docker service is down", timeout));
    }

    private static boolean tryCreateDummyTable(LocalQueryRunner localQueryRunner)
    {
        try {
            localQueryRunner.execute("CREATE TABLE dummy_nation AS SELECT * FROM tpch.tiny.nation");
            return true;
        }
        catch (RuntimeException e) {
            String message = format("Failed to create dummy table: %s", e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug(message, e);
            }
            else {
                log.info(message);
            }
            return false;
        }
    }

    private static void importTables(LocalQueryRunner localQueryRunner, String... tables)
    {
        for (String table : tables) {
            localQueryRunner.execute(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.%s", table, table));
        }
    }

    private static void importTablesBucketed(LocalQueryRunner localQueryRunner, List<String> bucketedBy, String... tables)
    {
        for (String table : tables) {
            localQueryRunner.execute(format(
                    "CREATE TABLE %s_bucketed WITH (bucketed_by=array[%s], bucket_count=11) AS SELECT * FROM tpch.tiny.%s",
                    table,
                    bucketedBy.stream()
                            .map(value -> "'" + value + "'")
                            .collect(joining(",")),
                    table));
        }
    }

    /**
     * Spark has to deploy Presto on Spark package to every worker for every query.
     * Unfortunately Spark doesn't try to eagerly delete application data from the workers, and after running
     * a couple of queries the disk space utilization spikes.
     * While this might not be an issue when testing locally the disk space is usually very limited on CI environments.
     * To avoid issues when running on a CI environment we have to drop temporary application data eagerly after each test.
     */
    @AfterMethod(alwaysRun = true)
    public void cleanupSparkWorkDirectory()
            throws Exception
    {
        if (sparkWorkDirectory != null) {
            // Docker containers are run with a different user id. Run "rm" in a container to avoid permission related issues.
            int exitCode = dockerCompose.run(
                    "-v", format("%s:/spark/work", sparkWorkDirectory.getAbsolutePath()),
                    "spark-submit",
                    "/bin/bash", "-c", "rm -rf /spark/work/*");
            assertEquals(exitCode, 0);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (composeProcess != null) {
            destroyProcess(composeProcess);
            composeProcess = null;
        }
        if (dockerCompose != null) {
            dockerCompose.down();
            dockerCompose = null;
        }
        if (localQueryRunner != null) {
            localQueryRunner.close();
            localQueryRunner = null;
        }
        if (tempDir != null) {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
            tempDir = null;
        }
    }

    private void executeOnSpark(String query)
            throws InterruptedException, IOException
    {
        File queryFile = new File(tempDir, randomUUID() + ".sql");
        write(query.getBytes(UTF_8), queryFile);

        int exitCode = dockerCompose.run(
                "-v", format("%s:/presto/launcher.jar", prestoLauncher.getAbsolutePath()),
                "-v", format("%s:/presto/package.tar.gz", prestoPackage.getAbsolutePath()),
                "-v", format("%s:/presto/query.sql", queryFile.getAbsolutePath()),
                "-v", format("%s:/presto/etc/config.properties", configProperties.getAbsolutePath()),
                "-v", format("%s:/presto/etc/catalogs", catalogDirectory.getAbsolutePath()),
                "spark-submit",
                "/spark/bin/spark-submit",
                "--executor-memory", "512m",
                "--executor-cores", "4",
                "--conf", "spark.task.cpus=4",
                "--master", "spark://spark-master:7077",
                "--class", "com.facebook.presto.spark.launcher.PrestoSparkLauncher",
                "/presto/launcher.jar",
                "--package", "/presto/package.tar.gz",
                "--config", "/presto/etc/config.properties",
                "--catalogs", "/presto/etc/catalogs",
                "--catalog", "hive",
                "--schema", "default",
                "--file", "/presto/query.sql");
        assertEquals(exitCode, 0);
    }

    private static File extractResource(String resource, File destinationDirectory)
    {
        File file = new File(destinationDirectory, Paths.get(resource).getFileName().toString());
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(toByteArray(getResource(resource)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    private static File resolveProjectRoot()
    {
        File directory = new File(System.getProperty("user.dir"));
        while (true) {
            File prestoSparkTestingDirectory = new File(directory, "presto-spark-testing");
            if (prestoSparkTestingDirectory.exists() && prestoSparkTestingDirectory.isDirectory()) {
                return directory;
            }
            directory = directory.getParentFile();
            if (directory == null) {
                throw new IllegalStateException("working directory must be set to a directory within the presto project");
            }
        }
    }

    private static File resolveFile(File directory, Pattern pattern)
            throws FileNotFoundException
    {
        checkArgument(directory.exists() && directory.isDirectory(), "directory does not exist: %s", directory);
        List<File> result = new ArrayList<>();
        for (File file : directory.listFiles()) {
            if (pattern.matcher(file.getName()).matches()) {
                result.add(file);
            }
        }
        if (result.isEmpty()) {
            throw new FileNotFoundException(format("directory %s doesn't contain a file that matches the given pattern: %s", directory, pattern));
        }
        if (result.size() > 1) {
            throw new FileNotFoundException(format("directory %s contains multiple files that match the given pattern: %s", directory, pattern));
        }
        return getOnlyElement(result);
    }

    private static void logPackageInfo(File file)
            throws IOException
    {
        long lastModified = file.lastModified();
        log.info(
                "%s size: %s modified: %s sha256sum: %s",
                file,
                file.length(),
                new Date(lastModified),
                asByteSource(file).hash(Hashing.sha256()).toString());
        long minutesSinceLastModified = (System.currentTimeMillis() - lastModified) / 1000 / 60;
        if (minutesSinceLastModified > 30) {
            log.warn("%s was modified more than 30 minutes ago. " +
                    "This test doesn't trigger automatic build. " +
                    "After any changes are applied - the project must be completely rebuilt for the changes to take effect.", file);
        }
    }

    private static void storeProperties(File file, Map<String, String> properties)
            throws IOException
    {
        Properties p = new Properties();
        p.putAll(properties);
        try (OutputStream outputStream = new FileOutputStream(file)) {
            p.store(outputStream, "");
        }
    }

    @Test
    public void testAggregation()
            throws Exception
    {
        assertQuery("" +
                "SELECT partkey, count(*) c " +
                "FROM lineitem " +
                "WHERE partkey % 10 = 1 " +
                "GROUP BY partkey " +
                "HAVING count(*) = 42");
    }

    @Test
    public void testBucketedAggregation()
            throws Exception
    {
        assertQuery("" +
                "SELECT orderkey, count(*) c " +
                "FROM lineitem_bucketed " +
                "WHERE partkey % 10 = 1 " +
                "GROUP BY orderkey");
    }

    @Test
    public void testJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem l " +
                "JOIN orders o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");
    }

    @Test
    public void testBucketedJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem_bucketed l " +
                "JOIN orders_bucketed o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");
    }

    @Test
    public void testCrossJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT o.custkey, l.orderkey " +
                "FROM (SELECT * FROM lineitem  WHERE linenumber = 4) l " +
                "CROSS JOIN (SELECT * FROM orders WHERE orderkey = 5) o");
    }

    @Test
    public void testNWayJoin()
            throws Exception
    {
        assertQuery("SELECT " +
                "   l.orderkey, " +
                "   l.linenumber, " +
                "   o1.orderstatus as orderstatus1, " +
                "   o2.orderstatus as orderstatus2, " +
                "   o3.orderstatus as orderstatus3, " +
                "   o4.orderstatus as orderstatus4, " +
                "   o5.orderstatus as orderstatus5, " +
                "   o6.orderstatus as orderstatus6 " +
                "FROM lineitem l, orders o1, orders o2, orders o3, orders o4, orders o5, orders o6 " +
                "WHERE l.orderkey = o1.orderkey " +
                "AND l.orderkey = o2.orderkey " +
                "AND l.orderkey = o3.orderkey " +
                "AND l.orderkey = o4.orderkey " +
                "AND l.orderkey = o5.orderkey " +
                "AND l.orderkey = o6.orderkey");
    }

    @Test
    public void testUnionAll()
            throws Exception
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testValues()
            throws Exception
    {
        assertQuery("SELECT a, b " +
                "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) ");
    }

    @Test
    public void testUnionWithAggregationAndJoin()
            throws Exception
    {
        assertQuery(
                "SELECT t.orderkey, t.c, o.orderstatus  FROM ( " +
                        "SELECT orderkey, count(*) as c FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY orderkey) t " +
                        "JOIN orders o " +
                        "ON (o.orderkey = t.orderkey)");
    }

    @Test
    public void testBucketedTableWrite()
            throws Exception
    {
        executeOnSpark("CREATE TABLE test_orders_bucketed " +
                "WITH (bucketed_by=array['orderkey'], bucket_count=11) " +
                "AS SELECT * FROM orders_bucketed");
        MaterializedResult actual = localQueryRunner.execute("SELECT * FROM test_orders_bucketed");
        MaterializedResult expected = localQueryRunner.execute("SELECT * FROM orders_bucketed");
        assertEqualsIgnoreOrder(actual, expected);
        dropTable("test_orders_bucketed");
    }

    private void assertQuery(String query)
            throws Exception
    {
        String tableName = "__tmp_" + randomUUID().toString().replaceAll("-", "_");
        executeOnSpark(format("CREATE TABLE %s AS %s", tableName, query));
        MaterializedResult actual = localQueryRunner.execute(format("SELECT * FROM %s", tableName));
        MaterializedResult expected = localQueryRunner.execute(query);
        assertEqualsIgnoreOrder(actual, expected);
        dropTable(tableName);
    }

    private void dropTable(String table)
    {
        // LocalQueryRunner doesn't support DROP TABLE
        localQueryRunner.inTransaction(localQueryRunner.getDefaultSession(), transactionSession -> {
            Metadata metadata = localQueryRunner.getMetadata();
            TableHandle tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName("hive", "default", table)).get();
            metadata.dropTable(transactionSession, tableHandle);
            return null;
        });
    }
}
