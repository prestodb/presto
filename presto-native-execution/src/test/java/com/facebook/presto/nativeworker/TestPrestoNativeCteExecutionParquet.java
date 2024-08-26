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

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.facebook.presto.SystemSessionProperties.CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;

@Test(groups = {"parquet"})
public class TestPrestoNativeCteExecutionParquet
        extends AbstractTestNativeCteExecution
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createNativeCteQueryRunner(true, "PARQUET");
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeCteExecutionParquet]-Default session from actual query runner: " + session.getCatalog() + "--" + session.getSchema());
        // default addStorageFormatToPath is true
        Path hiveDataDirectory = Paths.get(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory() + "/" + FileFormat.PARQUET.name());
        System.out.println("--[TestPrestoNativeCteExecutionParquet]--actual query runner data directory: " + hiveDataDirectory.toString());
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET");
        Session session = queryRunner.getDefaultSession();
        System.out.println("--[TestPrestoNativeCteExecutionParquet]-Default session from expected query runner: " + session.getCatalog() + "--" + session.getSchema());
        // default addStorageFormatToPath is true
        Path icebergDataDirectory = Paths.get(((DistributedQueryRunner) queryRunner).getCoordinator().getDataDirectory() + "/" + FileFormat.PARQUET.name());
        System.out.println("--[TestPrestoNativeCteExecutionParquet]--expected query runner data directory: " + icebergDataDirectory.toString());
        return queryRunner;
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "NONE")
                .build();
    }

    @Override
    protected Session getMaterializedSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, "hive")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, "true")
                .build();
    }
}
