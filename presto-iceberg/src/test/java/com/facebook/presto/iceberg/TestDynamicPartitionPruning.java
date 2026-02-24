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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;

@Test(singleThreaded = true)
public class TestDynamicPartitionPruning
        extends AbstractTestDynamicPartitionPruning
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .build()
                .getQueryRunner();

        // Hive plugin for CTE/exchange materialization temp tables.
        // Shares the same file-based metastore as the Iceberg catalog.
        Path catalogDirectory = getIcebergDataDirectoryPath(
                queryRunner.getCoordinator().getDataDirectory(),
                HIVE.name(), FileFormat.PARQUET, false);

        queryRunner.installPlugin(new HivePlugin("hive"));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir",
                        catalogDirectory.toFile().toURI().toString())
                .put("hive.temporary-table-schema", "tpch")
                .put("hive.temporary-table-storage-format", "PAGEFILE")
                .build());

        return queryRunner;
    }
}
