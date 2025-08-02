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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Module;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.METASTORE_CONTEXT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
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
    private static final Logger log = Logger.get(PrestoSparkNativeQueryRunnerUtils.class);

    private static final int AVAILABLE_CPU_COUNT = 4;
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";
    private static final String DEFAULT_STORAGE_FORMAT = "DWRF";
    private static Optional<Path> dataDirectory = Optional.empty();

    private PrestoSparkNativeQueryRunnerUtils() {}

    public static NativeQueryRunnerParameters getNativeQueryRunnerParameters()
    {
        Path prestoServerPath = Paths.get(getProperty("PRESTO_SERVER")
                        .orElse("_build/debug/presto_cpp/main/presto_server"))
                .toAbsolutePath();
        Path dataDirectory = Paths.get(getProperty("DATA_DIR")
                        .orElse("target/velox_data"))
                .toAbsolutePath();
        Optional<Integer> workerCount = getProperty("WORKER_COUNT").map(Integer::parseInt);

        assertTrue(Files.exists(prestoServerPath), format("Native worker binary at %s not found. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.", prestoServerPath));
        log.info("Using PRESTO_SERVER binary at %s", prestoServerPath);
<
        if (!Files.exists(dataDirectory)) {
            assertTrue(dataDirectory.toFile().mkdirs());
        }

        assertTrue(Files.exists(dataDirectory), format("Data directory at %s is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments to specify the path", dataDirectory));
        log.info("using DATA_DIR at %s", dataDirectory);

        return new NativeQueryRunnerParameters(prestoServerPath, dataDirectory, workerCount);
    }

    public static class NativeQueryRunnerParameters
    {
        public final Path serverBinary;
        public final Path dataDirectory;
        public final Optional<Integer> workerCount;

        public NativeQueryRunnerParameters(Path serverBinary, Path dataDirectory, Optional<Integer> workerCount)
        {
            this.serverBinary = requireNonNull(serverBinary, "serverBinary is null");
            this.dataDirectory = requireNonNull(dataDirectory, "dataDirectory is null");
            this.workerCount = requireNonNull(workerCount, "workerCount is null");
        }
    }

    public static Optional<String> getProperty(String name)
    {
        String systemPropertyValue = System.getProperty(name);
        String environmentVariableValue = System.getenv(name);
        if (systemPropertyValue == null) {
            if (environmentVariableValue == null) {
                return Optional.empty();
            }
            else {
                return Optional.of(environmentVariableValue);
            }
        }
        else {
            if (environmentVariableValue != null && !systemPropertyValue.equals(environmentVariableValue)) {
                throw new IllegalArgumentException(format("%s is set in both Java system property and environment variable, but their values are different. The Java system property value is %s, while the" +
                                " environment variable value is %s. Please use only one value.",
                        name,
                        systemPropertyValue,
                        environmentVariableValue));
            }
            return Optional.of(systemPropertyValue);
        }
    }

    public static void setupJsonFunctionNamespaceManager(QueryRunner queryRunner, String jsonFileName, String catalogName)
    {
        String jsonDefinitionPath = Resources.getResource(jsonFileName).getFile();
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        queryRunner.loadFunctionNamespaceManager(
                JsonFileBasedFunctionNamespaceManagerFactory.NAME,
                catalogName,
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "json-based-function-manager.path-to-function-definition", jsonDefinitionPath));
    }

    ///  NativeQueryRunnerUtils //////////////////
    /// START
    public static Map<String, String> getNativeWorkerHiveProperties()
    {
     return ImmutableMap.of("hive.parquet.pushdown-filter-enabled", "true",
             "hive.orc-compression-codec", "ZSTD",  "hive.storage-format", "DWRF");
    }

    public static Map<String, String> getNativeWorkerSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("native-execution-enabled", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("regex-library", "RE2J")
                .put("offset-clause-enabled", "true")
                // By default, Presto will expand some functions into its SQL equivalent (e.g. array_duplicates()).
                // With Velox, we do not want Presto to replace the function with its SQL equivalent.
                // To achieve that, we set inline-sql-functions to false.
                .put("inline-sql-functions", "false")
                .put("use-alternative-function-signatures", "true")
                .build();
    }
    /// END ////////////////////////////////////

    public static Map<String, String> getNativeExecutionSessionConfigs()
    {
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
                // Do not use default Prestissimo config files. Presto-Spark will generate the configs on-the-fly.
                .put("catalog.config-dir", "/")
                .put("task.info-update-interval", "100ms")
                .put("spark.initial-partition-count", "1")
                .put("register-test-functions", "true")
                .put("native-execution-program-arguments", "--logtostderr=1 --minloglevel=3")
                .put("spark.partition-count-auto-tune-enabled", "false");

        if (System.getProperty("NATIVE_PORT") == null) {
            builder.put("native-execution-executable-path", getNativeQueryRunnerParameters().serverBinary.toString());
        }

        try {
            builder.put("native-execution-broadcast-base-path",
                    Files.createTempDirectory("native_broadcast").toAbsolutePath().toString());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating temporary directory for broadcast", e);
        }

        return builder.build();
    }

    public static PrestoSparkQueryRunner createNativeHiveRunner()
    {
        PrestoSparkQueryRunner queryRunner = createNativeRunner("hive", new NativeExecutionModule());
        setupJsonFunctionNamespaceManager(queryRunner, "external_functions.json", "json");

        return queryRunner;
    }

    // Similar to createPrestoSparkNativeQueryRunner, but with custom connector config and without jsonFunctionNamespaceManager
    public static PrestoSparkQueryRunner createNativeTpchRunner()
    {
        return createNativeRunner(
                "tpchstandard",
                new NativeExecutionModule(
                        Optional.of(new NativeExecutionConnectorConfig().setConnectorName("tpch"))));
    }

    private static PrestoSparkQueryRunner createNativeRunner(String defaultCatalog, NativeExecutionModule nativeExecutionModule)
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

    public static PrestoSparkQueryRunner createRunner(String defaultCatalog, Optional<Path> baseDir, Map<String, String> additionalConfigProperties, Map<String, String> additionalSparkProperties, ImmutableList<Module> nativeModules)
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.putAll(getNativeWorkerSystemProperties()).putAll(additionalConfigProperties);
        Optional<Path> dataDir = baseDir.map(path -> Paths.get(path.toString() + '/' + DEFAULT_STORAGE_FORMAT));
        PrestoSparkQueryRunner queryRunner = new PrestoSparkQueryRunner(
                defaultCatalog,
                configBuilder.build(),
                getNativeWorkerHiveProperties(),
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

    public static void customizeLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.spark", WARN);
        logging.setLevel("com.facebook.presto.spark", WARN);
    }

    public static synchronized Path getBaseDataPath()
    {
        if (dataDirectory.isPresent()) {
            return dataDirectory.get();
        }

        dataDirectory = Optional.of(getNativeQueryRunnerParameters().dataDirectory);
        return dataDirectory.get();
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
}
