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
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.json.JsonFileBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.facebook.presto.common.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.HiveQueryRunner.createDatabaseMetastoreObject;
import static com.facebook.presto.hive.HiveQueryRunner.getFileHiveMetastore;
import static com.facebook.presto.hive.HiveTestUtils.getProperty;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeSidecarProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerTpcdsProperties;
import static com.facebook.presto.nativeworker.SymlinkManifestGeneratorUtils.createSymlinkManifest;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class PrestoNativeQueryRunnerUtils
{
    public enum QueryRunnerType
    {
        JAVA,
        NATIVE
    }

    // The unix domain socket (UDS) used to communicate with the remote function server.
    public static final String REMOTE_FUNCTION_UDS = "remote_function_server.socket";
    public static final String REMOTE_FUNCTION_JSON_SIGNATURES = "remote_function_server.json";
    public static final String REMOTE_FUNCTION_CATALOG_NAME = "remote";
    public static final String HIVE_DATA = "hive_data";

    protected static final String ICEBERG_DEFAULT_STORAGE_FORMAT = "PARQUET";

    private static final Logger log = Logger.get(PrestoNativeQueryRunnerUtils.class);
    private static final String DEFAULT_STORAGE_FORMAT = "DWRF";
    private static final String SYMLINK_FOLDER = "symlink_tables_manifests";
    private static final PrincipalPrivileges PRINCIPAL_PRIVILEGES = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
    private static final ErrorCode CREATE_ERROR_CODE = new ErrorCode(123, "CREATE_ERROR_CODE", INTERNAL_ERROR);

    private static final StorageFormat STORAGE_FORMAT_SYMLINK_TABLE = StorageFormat.create(
            ParquetHiveSerDe.class.getName(),
            SymlinkTextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName());

    private PrestoNativeQueryRunnerUtils() {}

    public static HiveQueryRunnerBuilder nativeHiveQueryRunnerBuilder()
    {
        return new HiveQueryRunnerBuilder(QueryRunnerType.NATIVE);
    }

    public static HiveQueryRunnerBuilder javaHiveQueryRunnerBuilder()
    {
        return new HiveQueryRunnerBuilder(QueryRunnerType.JAVA);
    }

    public static class HiveQueryRunnerBuilder
    {
        private NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        private Path dataDirectory = nativeQueryRunnerParameters.dataDirectory;
        private String serverBinary = nativeQueryRunnerParameters.serverBinary.toString();
        private Integer workerCount = nativeQueryRunnerParameters.workerCount.orElse(4);
        private Integer cacheMaxSize = 0;
        private String storageFormat = DEFAULT_STORAGE_FORMAT;
        private Optional<String> remoteFunctionServerUds = Optional.empty();
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> extraCoordinatorProperties = new HashMap<>();
        private Map<String, String> hiveProperties = new HashMap<>();
        private Map<String, String> tpcdsProperties = new HashMap<>();
        private String security;
        private boolean addStorageFormatToPath;
        private boolean coordinatorSidecarEnabled;
        private boolean builtInWorkerFunctionsEnabled;
        private boolean enableRuntimeMetricsCollection;
        private boolean enableSsdCache;
        private boolean failOnNestedLoopJoin;
        private boolean implicitCastCharNToVarchar;
        // External worker launcher is applicable only for the native hive query runner, since it depends on other
        // properties it should be created once all the other query runner configs are set. This variable indicates
        // whether the query runner returned by builder should use an external worker launcher, it will be true only
        // for the native query runner and should NOT be explicitly configured by users.
        private boolean useExternalWorkerLauncher;

        private HiveQueryRunnerBuilder(QueryRunnerType queryRunnerType)
        {
            this.hiveProperties.putAll(ImmutableMap.<String, String>builder()
                    .put("hive.storage-format", this.storageFormat)
                    .put("hive.pushdown-filter-enabled", "true")
                    .build());
            this.tpcdsProperties.putAll(getNativeWorkerTpcdsProperties());

            if (queryRunnerType.equals(QueryRunnerType.NATIVE)) {
                this.extraProperties.putAll(getNativeWorkerSystemProperties());
                this.extraCoordinatorProperties.putAll(ImmutableMap.<String, String>builder()
                        .put("native-execution-enabled", "true")
                        .put("http-server.http.port", "8081")
                        .build());
                // The property "hive.allow-drop-table" needs to be set to true because security is always "legacy" in NativeQueryRunner.
                this.hiveProperties.putAll(ImmutableMap.<String, String>builder()
                        .putAll(getNativeWorkerHiveProperties())
                        .put("hive.allow-drop-table", "true")
                        .build());
                this.security = "legacy";
                this.useExternalWorkerLauncher = true;
            }
            else {
                this.extraProperties.putAll(ImmutableMap.of(
                        "regex-library", "RE2J",
                        "offset-clause-enabled", "true"));
                this.security = "sql-standard";
                this.useExternalWorkerLauncher = false;
            }
        }

        public HiveQueryRunnerBuilder setRemoteFunctionServerUds(Optional<String> remoteFunctionServerUds)
        {
            this.remoteFunctionServerUds = remoteFunctionServerUds;
            return this;
        }

        public HiveQueryRunnerBuilder setFailOnNestedLoopJoin(boolean failOnNestedLoopJoin)
        {
            this.failOnNestedLoopJoin = failOnNestedLoopJoin;
            return this;
        }

        public HiveQueryRunnerBuilder setUseThrift(boolean useThrift)
        {
            this.extraProperties.put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift));
            this.extraProperties.put("experimental.internal-communication.task-info-thrift-transport-enabled", String.valueOf(useThrift));
            this.extraProperties.put("experimental.internal-communication.task-update-request-thrift-serde-enabled", String.valueOf(useThrift));
            this.extraProperties.put("experimental.internal-communication.task-info-response-thrift-serde-enabled", String.valueOf(useThrift));
            return this;
        }

        public HiveQueryRunnerBuilder setUseReactorNettyHttpClient(boolean useReactorNettyHttpClient)
        {
            this.extraProperties
                    .put("reactor.netty-http-client-enabled", String.valueOf(useReactorNettyHttpClient));
            return this;
        }

        public HiveQueryRunnerBuilder setSingleNodeExecutionEnabled(boolean singleNodeExecutionEnabled)
        {
            if (singleNodeExecutionEnabled) {
                this.extraCoordinatorProperties.put("single-node-execution-enabled", "true");
            }
            return this;
        }

        public HiveQueryRunnerBuilder setEnableSsdCache(boolean enableSsdCache)
        {
            this.enableSsdCache = enableSsdCache;
            return this;
        }

        public HiveQueryRunnerBuilder setEnableRuntimeMetricsCollection(boolean enableRuntimeMetricsCollection)
        {
            this.enableRuntimeMetricsCollection = enableRuntimeMetricsCollection;
            return this;
        }

        public HiveQueryRunnerBuilder setCoordinatorSidecarEnabled(boolean coordinatorSidecarEnabled)
        {
            this.coordinatorSidecarEnabled = coordinatorSidecarEnabled;
            if (coordinatorSidecarEnabled) {
                this.extraProperties.putAll(getNativeSidecarProperties());
            }
            return this;
        }

        public HiveQueryRunnerBuilder setBuiltInWorkerFunctionsEnabled(boolean builtInWorkerFunctionsEnabled)
        {
            this.builtInWorkerFunctionsEnabled = builtInWorkerFunctionsEnabled;
            return this;
        }

        public HiveQueryRunnerBuilder setStorageFormat(String storageFormat)
        {
            this.storageFormat = storageFormat;
            this.hiveProperties.put("hive.storage-format", storageFormat);
            return this;
        }

        public HiveQueryRunnerBuilder setAddStorageFormatToPath(boolean addStorageFormatToPath)
        {
            this.addStorageFormatToPath = addStorageFormatToPath;
            return this;
        }

        public HiveQueryRunnerBuilder setCacheMaxSize(Integer cacheMaxSize)
        {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public HiveQueryRunnerBuilder setImplicitCastCharNToVarchar(boolean implicitCastCharNToVarchar)
        {
            this.implicitCastCharNToVarchar = implicitCastCharNToVarchar;
            return this;
        }

        public HiveQueryRunnerBuilder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties.putAll(extraProperties);
            return this;
        }

        public HiveQueryRunnerBuilder setExtraCoordinatorProperties(Map<String, String> extraCoordinatorProperties)
        {
            this.extraCoordinatorProperties.putAll(extraCoordinatorProperties);
            return this;
        }

        public HiveQueryRunnerBuilder setHiveProperties(Map<String, String> hiveProperties)
        {
            this.hiveProperties.putAll(hiveProperties);
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();
            if (this.useExternalWorkerLauncher) {
                externalWorkerLauncher = getExternalWorkerLauncher("hive", serverBinary, cacheMaxSize, remoteFunctionServerUds,
                        failOnNestedLoopJoin, coordinatorSidecarEnabled, builtInWorkerFunctionsEnabled, enableRuntimeMetricsCollection, enableSsdCache, implicitCastCharNToVarchar);
            }
            return HiveQueryRunner.createQueryRunner(
                    ImmutableList.of(),
                    ImmutableList.of(),
                    extraProperties,
                    extraCoordinatorProperties,
                    security,
                    hiveProperties,
                    Optional.ofNullable(workerCount),
                    Optional.of(Paths.get(addStorageFormatToPath ? dataDirectory.toString() + "/" + storageFormat : dataDirectory.toString())),
                    externalWorkerLauncher,
                    tpcdsProperties);
        }
    }

    public static IcebergQueryRunnerBuilder nativeIcebergQueryRunnerBuilder()
    {
        return new IcebergQueryRunnerBuilder(QueryRunnerType.NATIVE);
    }

    public static IcebergQueryRunnerBuilder javaIcebergQueryRunnerBuilder()
    {
        return new IcebergQueryRunnerBuilder(QueryRunnerType.JAVA);
    }

    public static class IcebergQueryRunnerBuilder
    {
        private NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        private Path dataDirectory = nativeQueryRunnerParameters.dataDirectory;
        private String serverBinary = nativeQueryRunnerParameters.serverBinary.toString();
        private Integer workerCount = nativeQueryRunnerParameters.workerCount.orElse(4);
        private Integer cacheMaxSize = 0;
        private String storageFormat = ICEBERG_DEFAULT_STORAGE_FORMAT;
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> extraConnectorProperties = new HashMap<>();
        private Optional<String> remoteFunctionServerUds = Optional.empty();
        private boolean addStorageFormatToPath;
        // External worker launcher is applicable only for the native iceberg query runner, since it depends on other
        // properties it should be created once all the other query runner configs are set. This variable indicates
        // whether the query runner returned by builder should use an external worker launcher, it will be true only
        // for the native query runner and should NOT be explicitly configured by users.
        private boolean useExternalWorkerLauncher;

        private IcebergQueryRunnerBuilder(QueryRunnerType queryRunnerType)
        {
            if (queryRunnerType.equals(QueryRunnerType.NATIVE)) {
                this.extraProperties.putAll(ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("query.max-stage-count", "110")
                        .putAll(getNativeWorkerSystemProperties())
                        .build());
                this.useExternalWorkerLauncher = true;
            }
            else {
                this.extraProperties.putAll(ImmutableMap.of(
                        "regex-library", "RE2J",
                        "offset-clause-enabled", "true",
                        "query.max-stage-count", "110"));
                this.extraConnectorProperties.putAll(ImmutableMap.of("hive.parquet.writer.version", "PARQUET_1_0"));
                this.useExternalWorkerLauncher = false;
            }
        }

        public IcebergQueryRunnerBuilder setStorageFormat(String storageFormat)
        {
            this.storageFormat = storageFormat;
            return this;
        }

        public IcebergQueryRunnerBuilder setAddStorageFormatToPath(boolean addStorageFormatToPath)
        {
            this.addStorageFormatToPath = addStorageFormatToPath;
            return this;
        }

        public IcebergQueryRunnerBuilder setUseThrift(boolean useThrift)
        {
            this.extraProperties
                    .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift));
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();
            if (this.useExternalWorkerLauncher) {
                externalWorkerLauncher = getExternalWorkerLauncher("iceberg", serverBinary, cacheMaxSize, remoteFunctionServerUds,
                        false, false, false, false, false, false);
            }
            return IcebergQueryRunner.builder()
                    .setExtraProperties(extraProperties)
                    .setExtraConnectorProperties(extraConnectorProperties)
                    .setFormat(FileFormat.valueOf(storageFormat))
                    .setCreateTpchTables(false)
                    .setAddJmxPlugin(false)
                    .setNodeCount(OptionalInt.of(workerCount))
                    .setExternalWorkerLauncher(externalWorkerLauncher)
                    .setAddStorageFormatToPath(addStorageFormatToPath)
                    .setDataDirectory(Optional.of(dataDirectory))
                    .setTpcdsProperties(getNativeWorkerTpcdsProperties())
                    .build().getQueryRunner();
        }
    }

    public static void createSchemaIfNotExist(QueryRunner queryRunner, String schemaName)
    {
        ExtendedHiveMetastore metastore = getFileHiveMetastore((DistributedQueryRunner) queryRunner);
        if (!metastore.getDatabase(METASTORE_CONTEXT, schemaName).isPresent()) {
            metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(schemaName));
        }
    }

    public static void createExternalTable(QueryRunner queryRunner, String sourceSchemaName, String tableName, List<Column> columns, String targetSchemaName)
    {
        ExtendedHiveMetastore metastore = getFileHiveMetastore((DistributedQueryRunner) queryRunner);
        File dataDirectory = ((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory().resolve(HIVE_DATA).toFile();
        Path hiveTableDataPath = dataDirectory.toPath().resolve(sourceSchemaName).resolve(tableName);
        Path symlinkTableDataPath = dataDirectory.toPath().getParent().resolve(SYMLINK_FOLDER).resolve(tableName);

        try {
            createSymlinkManifest(hiveTableDataPath, symlinkTableDataPath);
        }
        catch (IOException e) {
            throw new PrestoException(() -> CREATE_ERROR_CODE, "Failed to create symlink manifest file for table: " + tableName, e);
        }

        createSchemaIfNotExist(queryRunner, targetSchemaName);
        if (!metastore.getTable(METASTORE_CONTEXT, targetSchemaName, tableName).isPresent()) {
            metastore.createTable(METASTORE_CONTEXT, createHiveSymlinkTable(targetSchemaName, tableName, columns, symlinkTableDataPath.toString()), PRINCIPAL_PRIVILEGES, emptyList());
        }
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

        if (!Files.exists(dataDirectory)) {
            assertTrue(dataDirectory.toFile().mkdirs());
        }

        assertTrue(Files.exists(dataDirectory), format("Data directory at %s is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments to specify the path", dataDirectory));
        log.info("using DATA_DIR at %s", dataDirectory);

        return new NativeQueryRunnerParameters(prestoServerPath, dataDirectory, workerCount);
    }

    public static Optional<BiFunction<Integer, URI, Process>> getExternalWorkerLauncher(
            String catalogName,
            String prestoServerPath,
            int cacheMaxSize,
            Optional<String> remoteFunctionServerUds,
            Boolean failOnNestedLoopJoin,
            boolean isCoordinatorSidecarEnabled,
            boolean isBuiltInWorkerFunctionsEnabled,
            boolean enableRuntimeMetricsCollection,
            boolean enableSsdCache,
            boolean implicitCastCharNToVarchar)
    {
        return
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path dir = Paths.get("/tmp", PrestoNativeQueryRunnerUtils.class.getSimpleName());
                        Files.createDirectories(dir);
                        Path tempDirectoryPath = Files.createTempDirectory(dir, "worker");
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());

                        // Write config file - use an ephemeral port for the worker.
                        String configProperties = format("discovery.uri=%s%n" +
                                "presto.version=testversion%n" +
                                "system-memory-gb=4%n" +
                                "http-server.http.port=0%n", discoveryUri);

                        if (isCoordinatorSidecarEnabled) {
                            configProperties = format("%s%n" +
                                    "native-sidecar=true%n" +
                                    "presto.default-namespace=native.default%n", configProperties);
                        }
                        else if (isBuiltInWorkerFunctionsEnabled) {
                            configProperties = format("%s%n" +
                                    "native-sidecar=true%n", configProperties);
                        }

                        if (enableRuntimeMetricsCollection) {
                            configProperties = format("%s%n" +
                                    "runtime-metrics-collection-enabled=true%n", configProperties);
                        }

                        if (enableSsdCache) {
                            Path ssdCacheDir = Paths.get(tempDirectoryPath + "/velox-ssd-cache");
                            Files.createDirectories(ssdCacheDir);
                            configProperties = format("%s%n" +
                                    "async-cache-ssd-gb=1%n" +
                                    "async-cache-ssd-path=%s/%n", configProperties, ssdCacheDir);
                        }

                        if (remoteFunctionServerUds.isPresent()) {
                            String jsonSignaturesPath = Resources.getResource(REMOTE_FUNCTION_JSON_SIGNATURES).getFile();
                            configProperties = format("%s%n" +
                                    "remote-function-server.catalog-name=%s%n" +
                                    "remote-function-server.thrift.uds-path=%s%n" +
                                    "remote-function-server.serde=presto_page%n" +
                                    "remote-function-server.signature.files.directory.path=%s%n", configProperties, REMOTE_FUNCTION_CATALOG_NAME, remoteFunctionServerUds.get(), jsonSignaturesPath);
                        }

                        if (failOnNestedLoopJoin) {
                            configProperties = format("%s%n" + "velox-plan-validator-fail-on-nested-loop-join=true%n", configProperties);
                        }

                        if (implicitCastCharNToVarchar) {
                            configProperties = format("%s%n" + "char-n-to-varchar-implicit-cast=true%n", configProperties);
                        }

                        Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s%n" +
                                        "node.internal-address=127.0.0.1%n" +
                                        "node.environment=testing%n" +
                                        "node.location=test-location", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        if (cacheMaxSize > 0) {
                            Files.write(catalogDirectoryPath.resolve(format("%s.properties", catalogName)),
                                    format("connector.name=hive%n" +
                                            "cache.enabled=true%n" +
                                            "cache.max-cache-size=%s", cacheMaxSize).getBytes());
                        }
                        else {
                            Files.write(catalogDirectoryPath.resolve(format("%s.properties", catalogName)),
                                    "connector.name=hive".getBytes());
                        }
                        // Add catalog with caching always enabled.
                        Files.write(catalogDirectoryPath.resolve(format("%scached.properties", catalogName)),
                                format("connector.name=hive%n" +
                                        "cache.enabled=true%n" +
                                        "cache.max-cache-size=32").getBytes());

                        // Add a tpch catalog.
                        Files.write(catalogDirectoryPath.resolve("tpchstandard.properties"),
                                format("connector.name=tpch%n").getBytes());

                        // Disable stack trace capturing as some queries (using TRY) generate a lot of exceptions.
                        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1", "--velox_ssd_odirect=false")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("worker." + workerIndex + ".out").toFile()))
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
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

    private static Table createHiveSymlinkTable(String databaseName, String tableName, List<Column> columns, String location)
    {
        return new Table(
                Optional.of("catalogName"),
                databaseName,
                tableName,
                "hive",
                EXTERNAL_TABLE,
                new Storage(STORAGE_FORMAT_SYMLINK_TABLE,
                        "file:" + location,
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());
    }
}
