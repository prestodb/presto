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

import com.facebook.presto.iceberg.AbstractTestIcebergAnalyzeUppercaseColumns;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.apache.iceberg.catalog.Catalog;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

/**
 * E2E tests verifying that ANALYZE succeeds on Iceberg tables registered into a Presto
 * Hive-metastore catalog whose schemas were written by an external engine (e.g. Spark)
 * with uppercase column names — for both unpartitioned and identity-partitioned tables.
 */
@Test
public class TestIcebergHiveAnalyzeUppercaseColumns
        extends AbstractTestIcebergAnalyzeUppercaseColumns
{
    private static final String TABLE_NAME = "test_hive_uppercase_columns";
    private static final String PARTITIONED_TABLE_NAME = "test_hive_uppercase_columns_partitioned";

    /** Separate temp dir used as the "external" Hadoop warehouse (simulates Spark). */
    private File hadoopWarehouseLocation;

    @Override
    protected String getTableName()
    {
        return TABLE_NAME;
    }

    @Override
    protected String getPartitionedTableName()
    {
        return PARTITIONED_TABLE_NAME;
    }

    @Override
    protected File getHadoopWarehouseLocation()
    {
        return hadoopWarehouseLocation;
    }

    @BeforeClass
    public void init()
            throws Exception
    {
        hadoopWarehouseLocation = Files.newTemporaryFolder();

        super.init();
        assertQuerySucceeds("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);

        Catalog hadoopCatalog = loadHadoopCatalog();
        setupUnpartitionedTable(hadoopCatalog, TABLE_NAME);
        setupPartitionedTable(hadoopCatalog, PARTITIONED_TABLE_NAME);
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        unregisterTableQuietly(TABLE_NAME);
        unregisterTableQuietly(PARTITIONED_TABLE_NAME);
        deleteRecursively(hadoopWarehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }
}
