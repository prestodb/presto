package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;

public class HiveExternalWorkerQueryRunner
{
    private static final Logger log = Logger.get(HiveQueryRunner.class);

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        String prestoServerPath = System.getenv("PRESTO_SERVER");
        String baseDataDir = System.getenv("DATA_DIR");
        String workerCount = System.getenv("WORKER_COUNT");

        return createQueryRunner(Optional.ofNullable(prestoServerPath),
                Optional.ofNullable(baseDataDir).map(Paths::get),
                Optional.ofNullable(workerCount).map(Integer::parseInt));
    }

    public static DistributedQueryRunner createQueryRunner(Optional<String> prestoServerPath,
            Optional<Path> baseDataDir,
            Optional<Integer> workerCount)
            throws Exception
    {
        if (prestoServerPath.isPresent()) {
            checkArgument(baseDataDir.isPresent(), "Path to data files must be specified when testing external workers");
        }

        DistributedQueryRunner defaultQueryRunner = HiveQueryRunner.createQueryRunner(
                ImmutableList.of(NATION),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of("hive.storage-format", "DWRF"),
                baseDataDir);

        // DWRF doesn't support date type. Convert date columns to varchar for lineitem and orders.
        createLineitem(defaultQueryRunner);
        createOrders(defaultQueryRunner);

        if (!prestoServerPath.isPresent()) {
            return defaultQueryRunner;
        }

        defaultQueryRunner.close();

        // Make query runner with external workers for tests
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of(),
                ImmutableMap.of("optimizer.optimize-hash-generation", "false",
                        "parse-decimal-literals-as-double", "true",
                        "http-server.http.port", "8080"),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of("hive.storage-format", "DWRF"),
                workerCount,
                baseDataDir,
                Optional.of((workerIndex, discoveryUri) -> {
                    try {
                        Path tempDirectoryPath = Files.createTempDirectory(TestHiveExternalWorkersQueries.class.getSimpleName());
                        int port = 1234 + workerIndex;

                        // Write config files
                        Files.write(tempDirectoryPath.resolve("config.properties"),
                                format("discovery.uri=%s\n" +
                                        "presto.version=testversion\n" +
                                        "http-server.http.port=%d", discoveryUri, port).getBytes());
                        Files.write(tempDirectoryPath.resolve("node.properties"),
                                format("node.id=%s\n" +
                                        "node.ip=127.0.0.1\n" +
                                        "node.environment=testing", UUID.randomUUID()).getBytes());

                        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
                        Files.createDirectory(catalogDirectoryPath);
                        Files.write(catalogDirectoryPath.resolve("hive.properties"), "connector.name=hive".getBytes());

                        return new ProcessBuilder(prestoServerPath.get(), "--logtostderr=1", "--v=1")
                                .directory(tempDirectoryPath.toFile())
                                .redirectErrorStream(true)
                                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                                .redirectError(ProcessBuilder.Redirect.INHERIT)
                                .start();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));

        return queryRunner;
    }

    private static void createLineitem(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem")) {
            queryRunner.execute("CREATE TABLE lineitem AS " +
                    "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate, " +
                    "   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment " +
                    "FROM tpch.tiny.lineitem");
        }
    }

    private static void createOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders")) {
            queryRunner.execute("CREATE TABLE orders AS " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, cast(orderdate as varchar) as orderdate, " +
                    "   orderpriority, clerk, shippriority, comment " +
                    "FROM tpch.tiny.orders");
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work
        Logging.initialize();

        Optional<Path> baseDataDir = Optional.empty();
        if (args.length > 0) {
            if (args.length != 1) {
                log.error("usage: HiveQueryRunner [baseDataDir]\n");
                log.error("       [baseDataDir] is a local directory under which you want the hive_data directory to be created.]\n");
                System.exit(1);
            }

            File baseDataDirFile = new File(args[0]);
            if (baseDataDirFile.exists()) {
                if (!baseDataDirFile.isDirectory()) {
                    log.error("Error: " + baseDataDirFile.getAbsolutePath() + " is not a directory.");
                    System.exit(1);
                }
                else if (!baseDataDirFile.canRead() || !baseDataDirFile.canWrite()) {
                    log.error("Error: " + baseDataDirFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }
            else {
                // For user supplied path like [path_exists_but_is_not_readable_or_writable]/[paths_do_not_exist], the hadoop file system won't
                // be able to create directory for it. e.g. "/aaa/bbb" is not creatable because path "/" is not writable.
                while (!baseDataDirFile.exists()) {
                    baseDataDirFile = baseDataDirFile.getParentFile();
                }
                if (!baseDataDirFile.canRead() || !baseDataDirFile.canWrite()) {
                    log.error("Error: The ancestor directory " + baseDataDirFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }

            baseDataDir = Optional.of(baseDataDirFile.toPath());
        }

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) HiveExternalWorkerQueryRunner.createQueryRunner();
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
