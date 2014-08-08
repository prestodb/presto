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

import com.facebook.presto.benchmark.BenchmarkSuite;
import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.apache.hadoop.hive.metastore.api.Database;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.google.common.base.Preconditions.checkNotNull;

public final class HiveBenchmarkQueryRunner
{
    private HiveBenchmarkQueryRunner()
    {
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = checkNotNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        File tempDir = Files.createTempDir();
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner(tempDir)) {
            new BenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
        finally {
            FileUtils.deleteRecursively(tempDir);
        }
    }

    public static LocalQueryRunner createLocalQueryRunner(File tempDir)
    {
        ConnectorSession session = new ConnectorSession("user", "test", "hive", "tpch", UTC_KEY, Locale.ENGLISH, null, null);
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // add hive
        InMemoryHiveMetastore metastore = new InMemoryHiveMetastore();
        File tpchDataDir = new File(tempDir, "tpch");
        tpchDataDir.mkdir();
        metastore.createDatabase(new Database("tpch", null, tpchDataDir.toURI().toString(), null));

        HiveConnectorFactory hiveConnectorFactory = new HiveConnectorFactory(
                "hive",
                ImmutableMap.of("node.environment", "test"),
                HiveBenchmarkQueryRunner.class.getClassLoader(),
                metastore);

        Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://none.invalid:0")
                .put("hive.max-split-size", "10GB")
                .build();

        localQueryRunner.createCatalog("hive", hiveConnectorFactory, hiveCatalogConfig);

        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        return localQueryRunner;
    }
}
