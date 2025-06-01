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
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.cache.CacheBuilder;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSmokeHive
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHive()
    {
        super(HIVE);
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
}
