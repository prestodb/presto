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
package com.facebook.presto.ducklake;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import static java.lang.String.format;

/**
 * Starts a Presto server with a {@code ducklake} catalog (backed by a scratch PostgreSQL catalog
 * database loaded with the DuckLake test fixture) for manual, interactive sessions, e.g. with the
 * Presto CLI.
 * <p>
 * {@link TestingDuckLakeCatalog} defaults to a Docker-based {@code postgres:14} Testcontainers
 * instance; on a machine without Docker, pass {@code -Dducklake.test.postgres.mode=local} to use
 * a scratch {@code pg_ctl}-managed PostgreSQL 14 instance instead, using the Homebrew PostgreSQL
 * 14 binaries at {@code /opt/homebrew/opt/postgresql@14/bin} (override with {@code
 * -Dducklake.test.pg-bin}); a Homebrew PostgreSQL 14 install is then required on the local
 * machine.
 */
public final class DuckLakeLocalRunner
{
    private static final Logger log = Logger.get(DuckLakeLocalRunner.class);

    private DuckLakeLocalRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        int nodeCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;

        DuckLakeQueryRunner duckLakeQueryRunner = DuckLakeQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setNodeCount(nodeCount)
                .build();

        DistributedQueryRunner queryRunner = duckLakeQueryRunner.getQueryRunner();
        log.info("======== SERVER STARTED ========");
        log.info(format("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl()));
        log.info("DuckLake fixture data path: %s", duckLakeQueryRunner.getTestingCatalog().getDataPath());
        log.info("Example: SHOW SCHEMAS FROM ducklake");
        log.info("Example: SELECT * FROM ducklake.tpch.\"orders$snapshots\"");

        Thread.sleep(Long.MAX_VALUE);
    }
}
