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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logging;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Module;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.METASTORE_CONTEXT;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Following JVM argument is needed to run Spark native tests.
 * <p>
 * - PRESTO_SERVER
 * - This tells Spark where to find the Presto native binary to launch the process.
 * Example: -DPRESTO_SERVER=/path/to/native/process/bin
 * <p>
 * - DATA_DIR
 * - Optional path to store TPC-H tables used in the test. If this directory is empty, it will be
 * populated. If tables already exists, they will be reused.
 * <p>
 * Tests can be running in Interactive Debugging Mode that allows for easier debugging
 * experience. Instead of launching its own native process, the test will connect to an existing
 * native process. This gives developers flexibility to connect IDEA and debuggers to the native process.
 * Enable this mode by setting NATIVE_PORT JVM argument.
 * <p>
 * - NATIVE_PORT
 * - This is the port your externally launched native process listens to. It is used to tell Spark where to send
 * requests. This port number has to be the same as to which your externally launched process listens.
 * Example: -DNATIVE_PORT=7777.
 * When NATIVE_PORT is specified, PRESTO_SERVER argument is not requires and is ignored if specified.
 * <p>
 * For test queries requiring shuffle, the disk-based local shuffle will be used.
 */
public class PrestoSparkNativeQueryRunnerUtils
{
    private static final int AVAILABLE_CPU_COUNT = 4;
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";
    private static final String DEFAULT_STORAGE_FORMAT = "DWRF";
    private static Optional<Path> dataDirectory = Optional.empty();

    private PrestoSparkNativeQueryRunnerUtils() {}

    public static Map<String, String> getNativeExecutionSessionConfigs()
    {
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
                // Do not use default Prestissimo config files. Presto-Spark will generate the configs on-the-fly.
                .put("catalog.config-dir", "/")
                .put("task.info-update-interval", "100ms")
                .put("native-execution-enabled", "true")
                .put("spark.initial-partition-count", "1")
                .put("register-test-functions", "true")
                .put("native-execution-program-arguments", "--logtostderr=1 --minloglevel=3")
                .put("spark.partition-count-auto-tune-enabled", "false");

        if (System.getProperty("NATIVE_PORT") == null) {
            String path = requireNonNull(System.getProperty("PRESTO_SERVER"),
                    "Native worker binary path is missing. " +
                            "Add -DPRESTO_SERVER=/path/to/native/process/bin to your JVM arguments.");
            builder.put("native-execution-executable-path", path);
        }

        return builder.build();
    }

    public static PrestoSparkQueryRunner createHiveRunner()
    {
        PrestoSparkQueryRunner queryRunner = createRunner("hive", new NativeExecutionModule());
        setupJsonFunctionNamespaceManager(queryRunner);

        return queryRunner;
    }

    private static PrestoSparkQueryRunner createRunner(String defaultCatalog, NativeExecutionModule nativeExecutionModule)
    {
        // Increases log level to reduce log spamming while running test.
        customizeLogging();
        return createRunner(
                defaultCatalog,
                Optional.of(getBaseDataPath()),
                getNativeExecutionSessionConfigs(),
                getNativeExecutionShuffleConfigs(),
                ImmutableList.of(nativeExecutionModule));
    }

    // Similar to createPrestoSparkNativeQueryRunner, but with custom connector config and without jsonFunctionNamespaceManager
    public static PrestoSparkQueryRunner createTpchRunner()
    {
        return createRunner(
                "tpchstandard",
                new NativeExecutionModule(
                        Optional.of(new NativeExecutionConnectorConfig().setConnectorName("tpch"))));
    }

    public static PrestoSparkQueryRunner createRunner(String defaultCatalog, Optional<Path> baseDir, Map<String, String> additionalConfigProperties, Map<String, String> additionalSparkProperties, ImmutableList<Module> nativeModules)
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.putAll(getNativeWorkerSystemProperties()).putAll(additionalConfigProperties);
        Optional<Path> dataDir = baseDir.map(path -> Paths.get(path.toString() + '/' + DEFAULT_STORAGE_FORMAT));
        PrestoSparkQueryRunner queryRunner = new PrestoSparkQueryRunner(
                defaultCatalog,
                configBuilder.build(),
                getNativeWorkerHiveProperties(DEFAULT_STORAGE_FORMAT),
                additionalSparkProperties,
                dataDir,
                nativeModules,
                AVAILABLE_CPU_COUNT);

        ExtendedHiveMetastore metastore = queryRunner.getMetastore();
        if (!metastore.getDatabase(METASTORE_CONTEXT, "tpch").isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject("tpch"));
        }
        return queryRunner;
    }

    public static QueryRunner createJavaQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner(Optional.of(getBaseDataPath()), "legacy", DEFAULT_STORAGE_FORMAT);
    }

    public static void assertShuffleMetadata()
    {
        assertNotNull(SparkEnv.get());
        assertTrue(SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager);
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Set<Integer> partitions = shuffleManager.getAllPartitions();

        for (Integer partition : partitions) {
            Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(partition);
            assertTrue(shuffleHandle.isPresent());
            assertTrue(shuffleHandle.get() instanceof BypassMergeSortShuffleHandle);
        }
    }

    public static void customizeLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.spark", WARN);
        logging.setLevel("com.facebook.presto.spark", WARN);
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    private static Map<String, String> getNativeExecutionShuffleConfigs()
    {
        ImmutableMap.Builder<String, String> sparkConfigs = ImmutableMap.builder();
        sparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        sparkConfigs.put(FALLBACK_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return sparkConfigs.build();
    }

    public static void setupJsonFunctionNamespaceManager(QueryRunner queryRunner)
    {
        String jsonDefinitionPath = Resources.getResource("external_functions.json").getFile();
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                "json",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "json-based-function-manager.path-to-function-definition", jsonDefinitionPath));
    }

    public static synchronized Path getBaseDataPath()
    {
        if (dataDirectory.isPresent()) {
            return dataDirectory.get();
        }
        String dataDirectoryStr = System.getProperty("DATA_DIR");
        if (dataDirectoryStr.isEmpty()) {
            try {
                dataDirectory = Optional.of(createTempDirectory("PrestoTest").toAbsolutePath());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            dataDirectory = Optional.of(Paths.get(dataDirectoryStr));
        }
        return dataDirectory.get();
    }
}
