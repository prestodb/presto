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

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Starts a {@link DistributedQueryRunner} with a {@code ducklake} catalog backed by a fresh
 * {@link TestingDuckLakeCatalog} (PostgreSQL catalog database loaded with the DuckLake test
 * fixture) and a {@code tpch} catalog so results can be cross-checked against {@code tpch.tiny}.
 * Modeled on {@code com.facebook.presto.iceberg.IcebergQueryRunner}.
 */
public final class DuckLakeQueryRunner
{
    public static final String DUCKLAKE_CATALOG = "ducklake";

    private final DistributedQueryRunner queryRunner;
    private final TestingDuckLakeCatalog testingCatalog;

    private DuckLakeQueryRunner(DistributedQueryRunner queryRunner, TestingDuckLakeCatalog testingCatalog)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.testingCatalog = requireNonNull(testingCatalog, "testingCatalog is null");
    }

    public DistributedQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public TestingDuckLakeCatalog getTestingCatalog()
    {
        return testingCatalog;
    }

    /**
     * Closes the query runner (and therefore the {@code ducklake} connector) before closing the
     * testing catalog, so PostgreSQL is never torn down while a connector is still live.
     */
    public void close()
    {
        queryRunner.close();
        testingCatalog.close();
    }

    public static DuckLakeQueryRunner createQueryRunner()
            throws Exception
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder() {}

        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> extraConnectorProperties = new HashMap<>();
        private int nodeCount = 2;
        private String schema = "tpch";

        public Builder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = extraProperties;
            return this;
        }

        public Builder setExtraConnectorProperties(Map<String, String> extraConnectorProperties)
        {
            this.extraConnectorProperties = extraConnectorProperties;
            return this;
        }

        public Builder setNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public DuckLakeQueryRunner build()
                throws Exception
        {
            TestingDuckLakeCatalog testingCatalog = new TestingDuckLakeCatalog();
            try {
                Session session = testSessionBuilder()
                        .setCatalog(DUCKLAKE_CATALOG)
                        .setSchema(schema)
                        .build();

                DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                        .setExtraProperties(extraProperties)
                        .setNodeCount(nodeCount)
                        .build();

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.getServers().forEach(server -> {
                    MBeanServer mBeanServer = MBeanServerFactory.newMBeanServer();
                    server.installPlugin(new DuckLakePlugin(mBeanServer));
                });

                Map<String, String> connectorProperties = new HashMap<>();
                connectorProperties.put("ducklake.catalog.type", "POSTGRESQL");
                connectorProperties.put("ducklake.catalog.connection-url", testingCatalog.getJdbcUrl());
                connectorProperties.put("ducklake.catalog.connection-user", testingCatalog.getUser());
                if (!testingCatalog.getPassword().isEmpty()) {
                    connectorProperties.put("ducklake.catalog.connection-password", testingCatalog.getPassword());
                }
                connectorProperties.put("ducklake.catalog.schema", "public");
                connectorProperties.putAll(extraConnectorProperties);

                queryRunner.createCatalog(DUCKLAKE_CATALOG, "ducklake", ImmutableMap.copyOf(connectorProperties));

                return new DuckLakeQueryRunner(queryRunner, testingCatalog);
            }
            catch (Exception e) {
                testingCatalog.close();
                throw e;
            }
        }
    }
}
