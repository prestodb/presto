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
package com.facebook.presto.nativeworker;

import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.iceberg.AbstractTestDynamicPartitionPruning;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;

/**
 * Tests dynamic partition pruning with native C++ workers.
 * <p>
 * Uses the dual-runner pattern: a Java query runner creates and populates
 * Iceberg tables (with full partition stats via {@link #executeTableDdl}),
 * while the native runner executes test queries. This is necessary because
 * the C++ Iceberg writer does not populate min/max partition statistics.
 */
@Test(singleThreaded = true)
public class TestNativeDynamicPartitionPruning
        extends AbstractTestDynamicPartitionPruning
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner)
                PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                        .setStorageFormat("PARQUET")
                        .setUseThrift(true)
                        .setAdditionalCatalogs(ImmutableMap.of("hive", "hive"))
                        .build();

        // Hive plugin on coordinator for CTE/exchange materialization temp tables.
        // The Hive plugin runs on the coordinator only — native workers are unaffected.
        Path catalogDirectory = getIcebergDataDirectoryPath(
                queryRunner.getCoordinator().getDataDirectory(),
                HIVE.name(), FileFormat.PARQUET, false);

        queryRunner.installPlugin(new HivePlugin("hive"));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir",
                        catalogDirectory.toFile().toURI().toString())
                .put("hive.temporary-table-schema", "tpch")
                .put("hive.temporary-table-storage-format", "PARQUET")
                .put("hive.temporary-table-compression-codec", "NONE")
                .build());

        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .build();
    }

    /**
     * Routes DDL/DML through the Java query runner so that Iceberg partition
     * statistics (min/max) are properly populated.
     */
    @Override
    protected void executeTableDdl(String sql)
    {
        ((QueryRunner) getExpectedQueryRunner()).execute(sql);
    }

    @Override
    protected void executeTableDdl(String sql, long expectedRowCount)
    {
        ((QueryRunner) getExpectedQueryRunner()).execute(sql);
    }

}
