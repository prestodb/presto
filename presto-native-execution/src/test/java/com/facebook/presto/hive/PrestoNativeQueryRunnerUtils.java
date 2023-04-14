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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersHll;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartSupp;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerHiveProperties;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.getNativeWorkerSystemProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;

public class PrestoNativeQueryRunnerUtils
{
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
                cacheMaxSize);
    }

    public static QueryRunner createQueryRunner(
            Optional<String> prestoServerPath,
            Optional<Path> dataDirectory,
            Optional<Integer> workerCount,
            int cacheMaxSize)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(dataDirectory.isPresent(), "Path to data files must be specified when testing external workers");
        }

        QueryRunner defaultQueryRunner = createJavaQueryRunner(dataDirectory);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        return createNativeQueryRunner(dataDirectory.get().toString(), prestoServerPath.get(), workerCount, cacheMaxSize, true);
    }

    public static QueryRunner createJavaQueryRunner() throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        return createJavaQueryRunner(Optional.of(Paths.get(dataDirectory)));
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> dataDirectory)
            throws Exception
    {
        return createJavaQueryRunner(dataDirectory, "sql-standard");
    }

    public static QueryRunner createJavaQueryRunner(Optional<Path> dataDirectory, String security)
            throws Exception
    {
        DistributedQueryRunner queryRunner =
                HiveQueryRunner.createQueryRunner(
                        ImmutableList.of(),
                        ImmutableMap.of(
                                "parse-decimal-literals-as-double", "true",
                                "regex-library", "RE2J",
                                "offset-clause-enabled", "true"),
                        security,
                        ImmutableMap.of(
                                "hive.storage-format", "DWRF",
                                "hive.pushdown-filter-enabled", "true"),
                        dataDirectory);

        // DWRF doesn't support date type. Convert date columns to varchar for lineitem and orders.
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createOrdersHll(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createBucketedCustomer(queryRunner);
        createCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createSupplier(queryRunner);
        createEmptyTable(queryRunner);
        createPrestoBenchTables(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);

        return queryRunner;
    }

    public static QueryRunner createNativeQueryRunner(
            String dataDirectory,
            String prestoServerPath,
            Optional<Integer> workerCount,
            int cacheMaxSize,
            boolean useThrift)
            throws Exception
    {
        // Make query runner with external workers for tests
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.<String, String>builder()
                        .put("http-server.http.port", "8080")
                        .put("experimental.internal-communication.thrift-transport-enabled", String.valueOf(useThrift))
                        .putAll(getNativeWorkerSystemProperties())
                        .build(),
                ImmutableMap.of(),
                "legacy",
                getNativeWorkerHiveProperties(),
                workerCount,
                Optional.of(Paths.get(dataDirectory)),
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(PrestoNativeQueryRunnerUtils.class.getSimpleName());
                        Logger log = Logger.get(PrestoNativeQueryRunnerUtils.class);
                        log.info("Temp directory for Worker #%d: %s", workerIndex, tempDirectoryPath.toString());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("config.properties"),
                                format("discovery.uri=%s%n" +
                                        "presto.version=testversion%n" +
                                        "http_exec_threads=8%n" +
                                        "system-memory-gb=4%n" +
                                        "http-server.http.port=%d", discoveryUri, port).getBytes());
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

    public static QueryRunner createNativeQueryRunner(boolean useThrift)
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String dataDirectory = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath, "Native worker binary path is missing. Add -DPRESTO_SERVER=<path/to/presto_server> to your JVM arguments.");
        assertNotNull(dataDirectory, "Data directory path is missing. Add -DDATA_DIR=<path/to/data> to your JVM arguments.");

        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(dataDirectory, prestoServerPath, Optional.ofNullable(workerCount).map(Integer::parseInt), cacheMaxSize, useThrift);
    }
}
