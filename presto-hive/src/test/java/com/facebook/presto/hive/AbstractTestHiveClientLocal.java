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

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.io.Files;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;

import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTestHiveClientLocal
        extends AbstractTestHiveClient
{
    private static final String DEFAULT_TEST_DB_NAME = "test";

    private File tempDir;
    private String testDbName;

    protected AbstractTestHiveClientLocal()
    {
        this(DEFAULT_TEST_DB_NAME);
    }

    protected AbstractTestHiveClientLocal(String testDbName)
    {
        this.testDbName = requireNonNull(testDbName, "testDbName is null");
    }

    protected abstract ExtendedHiveMetastore createMetastore(File tempDir);

    @BeforeClass
    public void initialize()
    {
        tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createMetastore(tempDir);

        metastore.createDatabase(METASTORE_CONTEXT, Database.builder()
                .setDatabaseName(testDbName)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build());

        HiveClientConfig hiveConfig = new HiveClientConfig()
                .setTimeZone("America/Los_Angeles");
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();

        setup(testDbName, hiveConfig, new CacheConfig(), metastoreClientConfig, metastore);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        try {
            getMetastoreClient().dropDatabase(METASTORE_CONTEXT, testDbName);
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        if (tableName.getTableName().startsWith(TEMPORARY_TABLE_PREFIX)) {
            return super.getTableHandle(metadata, tableName);
        }
        throw new SkipException("tests using existing tables are not supported");
    }

    @Override
    public void testGetAllTableNames() {}

    @Override
    public void testGetAllTableColumns() {}

    @Override
    public void testGetAllTableColumnsInSchema() {}

    @Override
    public void testGetTableNames() {}

    @Override
    public void testGetTableSchemaOffline() {}
}
