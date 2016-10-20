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

import com.facebook.presto.Session;
import com.facebook.presto.hive.metastore.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.InMemoryHiveMetastore;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public final class HiveQueryRunner
{
    private static final Logger log = Logger.get(HiveQueryRunner.class);

    private HiveQueryRunner()
    {
    }

    public static final String HIVE_CATALOG = "hive";
    public static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    public static final String TPCH_SCHEMA = "tpch";
    private static final String TPCH_BUCKETED_SCHEMA = "tpch_bucketed";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("Asia/Kathmandu");

    public static DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(tables, ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties)
            throws Exception
    {
        return createQueryRunner(tables, extraProperties, "sql-standard", ImmutableMap.of());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties, String security, Map<String, String> extraHiveProperties)
            throws Exception
    {
        assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=Asia/Katmandu to your JVM arguments");

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4, extraProperties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
            InMemoryHiveMetastore metastore = new InMemoryHiveMetastore(baseDir);
            metastore.setUserRoles(createSession().getUser(), ImmutableSet.of(ADMIN_ROLE_NAME));
            metastore.createDatabase(createDatabaseMetastoreObject(baseDir, TPCH_SCHEMA));
            metastore.createDatabase(createDatabaseMetastoreObject(baseDir, TPCH_BUCKETED_SCHEMA));
            queryRunner.installPlugin(new HivePlugin(HIVE_CATALOG, new BridgingHiveMetastore(metastore)));

            metastore.setUserRoles(createSession().getUser(), ImmutableSet.of("admin"));

            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .putAll(extraHiveProperties)
                    .put("hive.metastore.uri", "thrift://localhost:8080")
                    .put("hive.time-zone", TIME_ZONE.getID())
                    .put("hive.security", security)
                    .build();
            Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                    .putAll(hiveProperties)
                    .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                    .put("hive.compression-codec", "NONE") // so that the file is splittable
                    .build();
            queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);
            queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, HIVE_CATALOG, hiveBucketedProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(), tables);

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private static Database createDatabaseMetastoreObject(File baseDir, String name)
    {
        Database database = new Database(name, null, new File(baseDir, name).toURI().toString(), null);
        database.setOwnerName("public");
        database.setOwnerType(PrincipalType.ROLE);
        return database;
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static Session createBucketedSession()
    {
        return testSessionBuilder()
                .setCatalog(HIVE_BUCKETED_CATALOG)
                .setSchema(TPCH_BUCKETED_SCHEMA)
                .build();
    }

    public static void copyTpchTablesBucketed(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableBucketed(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH)), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableBucketed(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql;
        switch (table.getObjectName()) {
            case "part":
            case "partsupp":
            case "supplier":
            case "nation":
            case "region":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['orderkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "customer":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        long rows = checkType(queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0), Long.class, "rows");
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(TpchTable.getTables(), ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
