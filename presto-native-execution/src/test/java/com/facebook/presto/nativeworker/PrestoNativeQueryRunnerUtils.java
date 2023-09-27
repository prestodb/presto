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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;

public class PrestoNativeQueryRunnerUtils
{
    private static final Logger log = Logger.get(PrestoNativeQueryRunnerUtils.class);

    private static final String DEFAULT_STORAGE_FORMAT = "DWRF";

    // The unix domain socket (UDS) used to communicate with the remote function server.
    public static final String REMOTE_FUNCTION_UDS = "remote_function_server.socket";
    public static final String REMOTE_FUNCTION_JSON_SIGNATURES = "remote_function_server.json";
    public static final String REMOTE_FUNCTION_CATALOG_NAME = "remote";

    private PrestoNativeQueryRunnerUtils() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getenv("PRESTO_SERVER");
        String dataDirectory = System.getenv("DATA_DIR");
        String workerCount = System.getenv("WORKER_COUNT");
        int cacheMaxSize = 4096; // 4GB size cache

        checkArgument(prestoServerPath != null, "Native worker binary path is missing. Add PRESTO_SERVER environment variable.");
        checkArgument(dataDirectory != null, "Data directory path is missing.. Add DATA_DIR environment variable.");

        return createQueryRunner(
                Optional.ofNullable(prestoServerPath),
                Optional.ofNullable(dataDirectory).map(Paths::get),
                Optional.ofNullable(workerCount).map(Integer::parseInt),
                cacheMaxSize,
                DEFAULT_STORAGE_FORMAT);
    }

    public static QueryRunner createQueryRunner(
            Optional<String> prestoServerPath,
            Optional<Path> dataDirectory,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            String storageFormat)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(dataDirectory.isPresent(), "Path to data files must be specified when testing external workers");
        }

        QueryRunner defaultQueryRunner = createJavaQueryRunner(dataDirectory, storageFormat);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        return createNativeQueryRunner(dataDirectory.get().toString(), prestoServerPath.get(), workerCount, cacheMaxSize, true, Optional.empty(), storageFormat);
    }

    public static QueryRunner createJavaQueryRunner() throws Exception
    {
        return createJavaQueryRunner(DEFAULT_STORAGE_FORMAT);
    }

    public static QueryRunner createJavaQueryRunner(String storageFormat) throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        return createJavaQueryRunner(Optional.of(Paths.get(dataDirectory)), storageFormat);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> dataDirectory, String storageFormat)
            throws Exception
    {
        return createJavaQueryRunner(dataDirectory, "sql-standard", storageFormat);
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> baseDataDirectory, String security, String storageFormat)
            throws Exception
    {
        ImmutableMap.Builder<String, String> hivePropertiesBuilder = new ImmutableMap.Builder<>();
        hivePropertiesBuilder
                .put("hive.storage-format", storageFormat)
                .put("hive.pushdown-filter-enabled", "true");

        if ("legacy".equals(security)) {
            hivePropertiesBuilder.put("hive.allow-drop-table", "true");
        }

        Optional<Path> dataDirectory = baseDataDirectory.map(path -> Paths.get(path.toString() + '/' + storageFormat));
        DistributedQueryRunner queryRunner =
                HiveQueryRunner.createQueryRunner(
                        ImmutableList.of(),
                        ImmutableMap.of(
                                "parse-decimal-literals-as-double", "true",
                                "regex-library", "RE2J",
                                "offset-clause-enabled", "true"),
                        security,
                        hivePropertiesBuilder.build(),
                        dataDirectory);
        return queryRunner;
    }

    public static QueryRunner createNativeQueryRunner(
            String dataDirectory,
            String prestoServerPath,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            boolean useThrift,
            Optional<String> remoteFunctionServerUds,
            String storageFormat)
            throws Exception
    {
        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .put("native-execution-enabled", "true")
                        .putAll(getNativeWorkerSystemProperties())
                        .build(),
                ImmutableMap.of(),
                "legacy",
                getNativeWorkerHiveProperties(storageFormat),
                workerCount,
                Optional.of(Paths.get(dataDirectory + "/" + storageFormat)),
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(PrestoNativeQueryRunnerUtils.class.getSimpleName());
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("velox.properties"), "".getBytes());
                        String configProperties = format("discovery.uri=%s%n" +
                                "presto.version=testversion%n" +
                                "http_exec_threads=8%n" +
                                "system-memory-gb=4%n" +
                                "http-server.http.port=%d", discoveryUri, port);

                        if (remoteFunctionServerUds.isPresent()) {
                            String jsonSignaturesPath = Resources.getResource(REMOTE_FUNCTION_JSON_SIGNATURES).getFile();
                            configProperties = format("%s%n" +
                                    "remote-function-server.catalog-name=%s%n" +
                                    "remote-function-server.thrift.uds-path=%s%n" +
                                    "remote-function-server.signature.files.directory.path=%s%n", configProperties, REMOTE_FUNCTION_CATALOG_NAME, remoteFunctionServerUds.get(), jsonSignaturesPath);
                        }
                        Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s%n" +
                                        "node.ip=127.0.0.1%n" +
                                        "node.environment=testing%n" +
                                        "node.location=test-location", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        if (cacheMaxSize > 0) {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive%n" +
                                           "cache.enabled=true%n" +
                                           "cache.max-cache-size=%s", cacheMaxSize).getBytes());
                        }
                        else {
                            Files.write(catalogDirectoryPath.resolve("hive.properties"),
                                    format("connector.name=hive").getBytes());
                        }
                        // Add a hive catalog with caching always enabled.
                        Files.write(catalogDirectoryPath.resolve("hivecached.properties"),
                                format("connector.name=hive%n" +
                                        "cache.enabled=true%n" +
                                        "cache.max-cache-size=32").getBytes());

                        // Add a tpch catalog.
                        Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                                format("connector.name=tpch%n").getBytes());

                        // Disable stack trace capturing as some queries (using TRY) generate a lot of exceptions.
                        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".err").toFile()))
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
    }

    public static QueryRunner createNativeQueryRunner(String remoteFunctionServerUds)
            throws Exception
    {
        return createNativeQueryRunner(false, DEFAULT_STORAGE_FORMAT, Optional.ofNullable(remoteFunctionServerUds));
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift)
            throws Exception
    {
        return createNativeQueryRunner(useThrift, DEFAULT_STORAGE_FORMAT);
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift, String storageFormat)
            throws Exception
    {
        return createNativeQueryRunner(useThrift, storageFormat, Optional.empty());
    }

    public static QueryRunner createNativeQueryRunner(boolean useThrift, String storageFormat, Optional<String> remoteFunctionServerUds)
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String dataDirectory = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath, "Native worker binary path is missing. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.");
        assertNotNull(dataDirectory, "Data directory path is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments.");

        return createNativeQueryRunner(dataDirectory, prestoServerPath, Optional.ofNullable(workerCount).map(Integer::parseInt), cacheMaxSize, useThrift, remoteFunctionServerUds, storageFormat);
    }

    // Start the remote function server. Return the UDS path used to communicate with it.
    public static String startRemoteFunctionServer(String remoteFunctionServerBinaryPath)
    {
        try {
            Path tempDirectoryPath = Files.createTempDirectory("RemoteFunctionServer");
            Path remoteFunctionServerUdsPath = tempDirectoryPath.resolve(REMOTE_FUNCTION_UDS);
            log.info("Temp directory for Remote Function Server: %s", tempDirectoryPath.toString());

            Process p = new ProcessBuilder(Paths.get(remoteFunctionServerBinaryPath).toAbsolutePath().toString(), "--uds_path", remoteFunctionServerUdsPath.toString(), "--function_prefix", REMOTE_FUNCTION_CATALOG_NAME + ".schema.")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("thrift_server.out").toFile()))
                                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("thrift_server.err").toFile()))
                                .start();
            return remoteFunctionServerUdsPath.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
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
}
