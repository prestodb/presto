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
package com.facebook.presto.delta;

import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiFunction;

import static com.facebook.airlift.log.Level.ERROR;
import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;

public class DeltaQueryRunner
{
    public static final String DELTA_CATALOG = "delta";
    public static final String HIVE_CATALOG = "hive";
    public static final String DELTA_SCHEMA = "deltaTables"; // Schema in Hive which has test Delta tables

    private DistributedQueryRunner queryRunner;

    private DeltaQueryRunner(DistributedQueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    public DistributedQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder() {}

        private Map<String, String> extraProperties = new HashMap<>();
        // If externalWorkerLauncher is not provided, Java workers are used by default.
        private Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();
        private TimeZoneKey timeZoneKey = UTC_KEY;
        private boolean caseSensitivePartitions;
        private OptionalInt nodeCount = OptionalInt.of(4);

        public Builder setExternalWorkerLauncher(Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
        {
            this.externalWorkerLauncher = requireNonNull(externalWorkerLauncher);
            return this;
        }

        public Builder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = ImmutableMap.copyOf(extraProperties);
            return this;
        }

        public Builder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey;
            return this;
        }

        public Builder caseSensitivePartitions()
        {
            caseSensitivePartitions = true;
            return this;
        }

        public Builder setNodeCount(OptionalInt nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public DeltaQueryRunner build()
                throws Exception
        {
            setupLogging();
            Session session = testSessionBuilder()
                    .setCatalog(DELTA_CATALOG)
                    .setSchema(DELTA_SCHEMA.toLowerCase(US))
                    .setTimeZoneKey(timeZoneKey)
                    .build();

            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                    .setExtraProperties(extraProperties)
                    .setNodeCount(nodeCount.orElse(4))
                    .setExternalWorkerLauncher(externalWorkerLauncher)
                    .build();

            // Install the TPCH plugin for test data (not in Delta format)
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Path dataDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("delta_metadata");
            Path catalogDirectory = dataDirectory.getParent().resolve("catalog");

            // Install a Delta connector catalog
            queryRunner.installPlugin(new DeltaPlugin());
            Map<String, String> deltaProperties = new HashMap<>();
            deltaProperties.put("hive.metastore", "file");
            deltaProperties.put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString());
            deltaProperties.put("delta.case-sensitive-partitions-enabled", Boolean.toString(caseSensitivePartitions));
            queryRunner.createCatalog(DELTA_CATALOG, "delta", deltaProperties);

            // Install a Hive connector catalog that uses the same metastore as Delta
            // This catalog will be used to create tables in metastore as the Delta connector doesn't
            // support creating tables yet.
            queryRunner.installPlugin(new HivePlugin("hive"));
            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", catalogDirectory.toFile().toURI().toString())
                    .put("hive.allow-drop-table", "true")
                    .put("hive.security", "legacy")
                    .build();
            queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
            queryRunner.execute(format("CREATE SCHEMA %s.%s", HIVE_CATALOG, DELTA_SCHEMA));

            return new DeltaQueryRunner(queryRunner);
        }
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("com.facebook.presto.event", WARN);
        logging.setLevel("com.facebook.presto.security.AccessControlManager", WARN);
        logging.setLevel("com.facebook.presto.server.PluginManager", WARN);
        logging.setLevel("com.facebook.airlift.bootstrap.LifeCycleManager", WARN);
        logging.setLevel("org.apache.parquet.hadoop", WARN);
        logging.setLevel("org.eclipse.jetty.server.handler.ContextHandler", WARN);
        logging.setLevel("org.eclipse.jetty.server.AbstractConnector", WARN);
        logging.setLevel("org.glassfish.jersey.internal.inject.Providers", ERROR);
        logging.setLevel("parquet.hadoop", WARN);
        logging.setLevel("org.apache.iceberg", WARN);
        logging.setLevel("com.facebook.airlift.bootstrap", WARN);
        logging.setLevel("Bootstrap", WARN);
        logging.setLevel("org.apache.hadoop.io.compress", WARN);
    }
}
