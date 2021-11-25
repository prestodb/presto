package com.facebook.presto.plugin.clickhouse;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import java.util.HashMap;
import java.util.Map;

public final class ClickHouseQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private ClickHouseQueryRunner() {}

    public static QueryRunner createClickHouseQueryRunner(TestingClickHouseServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createClickHouseQueryRunner(server, ImmutableMap.of(), ImmutableMap.of(), tables);
    }

    public static DistributedQueryRunner createClickHouseQueryRunner(
            TestingClickHouseServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("allow-drop-table", String.valueOf(true));
            connectorProperties.putIfAbsent("clickhouse.map-string-as-varchar", String.valueOf(true));

            queryRunner.installPlugin(new ClickHousePlugin());
            queryRunner.createCatalog("clickhouse", "clickhouse", connectorProperties);
            server.execute("CREATE DATABASE " + TPCH_SCHEMA);
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("clickhouse")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createClickHouseQueryRunner(
                new TestingClickHouseServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(ClickHouseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
