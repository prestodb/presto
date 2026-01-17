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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSmokeHive
        extends IcebergDistributedSmokeTestBase
{
    private static final String ANOTHER_CATALOG_NAME = "another_iceberg";
    private final Map<String, String> extraConnectorProperties;

    public TestIcebergSmokeHive()
    {
        super(HIVE);
        extraConnectorProperties = ImmutableMap.of("hive.metastore-cache-ttl", "2d",
                "hive.metastore-refresh-interval", "3d",
                "hive.metastore-cache-maximum-size", "10000000");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setExtraConnectorProperties(extraConnectorProperties)
                .build()
                .getQueryRunner();

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();
        Path catalogHiveDirectory = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false);
        Map<String, String> icebergHiveProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.file-format", "PARQUET")
                .put("iceberg.catalog.type", HIVE.name())
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogHiveDirectory.toFile().toURI().toString())
                .build();
        queryRunner.createCatalog(ANOTHER_CATALOG_NAME, "iceberg", icebergHiveProperties);
        return queryRunner;
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        Path dataDirectory = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory();
        File tempLocation = getIcebergDataDirectoryPath(dataDirectory, HIVE.name(), new IcebergConfig().getFileFormat(), false).toFile();
        return format("%s%s/%s", tempLocation.toURI(), schema, table);
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        IcebergFileHiveMetastore fileHiveMetastore = new IcebergFileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toString(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        String defaultCatalog = ((FullConnectorSession) session).getSession().getCatalog().get();
        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                session,
                new IcebergCatalogName(defaultCatalog),
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    @Test
    public void testShowCreateSchema()
    {
        String createSchemaSql = "CREATE SCHEMA show_create_iceberg_schema";
        assertUpdate(createSchemaSql);
        String expectedShowCreateSchema = "CREATE SCHEMA show_create_iceberg_schema\n" +
                "WITH (\n" +
                "   location = '.*show_create_iceberg_schema'\n" +
                ")";

        MaterializedResult actualResult = computeActual("SHOW CREATE SCHEMA show_create_iceberg_schema");
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet()).toString().matches(expectedShowCreateSchema));

        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s", getSession().getCatalog().get(), ""), ".*mismatched input '.'. Expecting: <EOF>");
        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s.%s", getSession().getCatalog().get(), "show_create_iceberg_schema", "tabletest"), ".*Too many parts in schema name: iceberg.show_create_iceberg_schema.tabletest");
        assertQueryFails(format("SHOW CREATE SCHEMA %s", "schema_not_exist"), ".*Schema 'iceberg.schema_not_exist' does not exist");
        assertUpdate("DROP SCHEMA show_create_iceberg_schema");
    }

    @Test
    public void testInsertAndQueryFromDifferentCatalogInstancesWithCache()
    {
        Session session = getQueryRunner().getDefaultSession();
        String schema = session.getSchema().get();
        String tableName = "test_insert";
        assertUpdate(session, format("create table %s (a int, b varchar)", tableName));

        assertQuery(session, "select count(*) from " + tableName, "values(0)");
        assertUpdate(session, format("insert into %s values(1, '1001'), (2, '1002')", tableName), 2);
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002')");

        Session anotherSession = Session.builder(getQueryRunner().getDefaultSession()).build();
        assertQuery(anotherSession, format("select * from %s.%s.%s", ANOTHER_CATALOG_NAME, schema, tableName), "values(1, '1001'), (2, '1002')");
        assertUpdate(anotherSession, format("insert into %s.%s.%s values(3, '1003'), (4, '1004')", ANOTHER_CATALOG_NAME, schema, tableName), 2);
        assertQuery(anotherSession, format("select * from %s.%s.%s", ANOTHER_CATALOG_NAME, schema, tableName), "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

        // This query shouldn't be affected by table metadata cache
        assertQuery(session, "select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");
        assertUpdate(session, "drop table " + tableName);
    }
}
