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

import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.io.Files;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;

import static io.airlift.testing.FileUtils.deleteRecursively;

public abstract class AbstractTestHiveClientLocal
        extends AbstractTestHiveClient
{
    private File tempDir;

    protected abstract ExtendedHiveMetastore createMetastore(File tempDir);

    @BeforeClass
    public void initialize()
    {
        tempDir = Files.createTempDir();

        ExtendedHiveMetastore metastore = createMetastore(tempDir);

        metastore.createDatabase(Database.builder()
                .setDatabaseName("test")
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build());

        HiveClientConfig hiveConfig = new HiveClientConfig()
                .setTimeZone("America/Los_Angeles");

        setup("test", hiveConfig, metastore);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        deleteRecursively(tempDir);
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
}
