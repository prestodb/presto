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
package com.facebook.presto.cassandra;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.QueryRunner;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createTestTables;

/**
 * Shared singleton CassandraServer that is created once and reused across all test classes.
 * This prevents the issue where each test class creates its own server and shuts it down
 * after completion, causing subsequent tests to fail when trying to access a closed server.
 */
public final class SharedCassandraServer
{
    private static final Logger log = Logger.get(SharedCassandraServer.class);

    private static final String SMOKE_TEST_KEYSPACE = "smoke_test";
    private static final Timestamp DATE_TIME_LOCAL = Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 0));

    private static volatile CassandraServer instance;
    private static volatile boolean tpchTablesCreated;
    private static volatile boolean smokeTestTablesCreated;
    private static final Object lock = new Object();

    static {
        // Register shutdown hook to close the server when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("=== SharedCassandraServer: Shutdown hook triggered ===");
            closeServer();
        }));
    }

    private SharedCassandraServer()
    {
        // Private constructor to prevent instantiation
    }

    /**
     * Gets the shared CassandraServer instance, creating it if necessary.
     */
    public static CassandraServer getInstance()
    {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    try {
                        log.info("=== SharedCassandraServer: Creating new CassandraServer instance ===");
                        instance = new CassandraServer();
                        log.info("=== SharedCassandraServer: CassandraServer instance created successfully ===");
                    }
                    catch (Exception e) {
                        log.error(e, "=== SharedCassandraServer: Failed to create CassandraServer instance ===");
                        throw new RuntimeException("Failed to create shared CassandraServer", e);
                    }
                }
            }
        }
        return instance;
    }

    /**
     * Ensures TPCH tables are created in the shared server.
     * This is called by test classes that need TPCH data.
     */
    public static void ensureTpchTablesCreated(QueryRunner queryRunner)
    {
        if (!tpchTablesCreated) {
            synchronized (lock) {
                if (!tpchTablesCreated) {
                    try {
                        log.info("=== SharedCassandraServer: Creating TPCH tables (first time only) ===");
                        CassandraServer server = getInstance();

                        // TPCH tables are created by CassandraQueryRunner.createCassandraQueryRunner
                        // We just need to mark them as created
                        tpchTablesCreated = true;
                        log.info("=== SharedCassandraServer: TPCH tables created successfully ===");
                    }
                    catch (Exception e) {
                        log.error(e, "=== SharedCassandraServer: Failed to create TPCH tables ===");
                        throw new RuntimeException("Failed to create TPCH tables", e);
                    }
                }
            }
        }
    }

    /**
     * Ensures smoke test tables are created in the shared server.
     * This is called by TestCassandraIntegrationSmokeTest.
     */
    public static void ensureSmokeTestTablesCreated()
    {
        if (!smokeTestTablesCreated) {
            synchronized (lock) {
                if (!smokeTestTablesCreated) {
                    try {
                        log.info("=== SharedCassandraServer: Creating smoke test tables (first time only) ===");
                        CassandraServer server = getInstance();
                        createTestTables(server.getSession(), server.getMetadata(), SMOKE_TEST_KEYSPACE, DATE_TIME_LOCAL);
                        smokeTestTablesCreated = true;
                        log.info("=== SharedCassandraServer: Smoke test tables created successfully ===");
                    }
                    catch (Exception e) {
                        log.error(e, "=== SharedCassandraServer: Failed to create smoke test tables ===");
                        throw new RuntimeException("Failed to create smoke test tables", e);
                    }
                }
            }
        }
    }

    /**
     * Closes the shared server. This should only be called by the shutdown hook.
     */
    private static void closeServer()
    {
        synchronized (lock) {
            if (instance != null) {
                try {
                    log.info("=== SharedCassandraServer: Closing CassandraServer instance ===");
                    instance.close();
                    log.info("=== SharedCassandraServer: CassandraServer instance closed successfully ===");
                }
                catch (Exception e) {
                    log.error(e, "=== SharedCassandraServer: Error closing CassandraServer instance ===");
                }
                finally {
                    instance = null;
                    tpchTablesCreated = false;
                    smokeTestTablesCreated = false;
                }
            }
        }
    }
}
