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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.TestIcebergRowLineageBase;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.DataProvider;

import java.io.File;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

/**
 * Runs the row lineage tests on the native query runner and compares each query with the Java
 * expected query runner, which share one HADOOP catalog directory. Every test method is
 * parameterized with {@code pushdown_filter_enabled=true/false} for the native side. The Java side
 * always runs with pushdown disabled, since the Java Iceberg connector rejects filter pushdown.
 */
public class TestIcebergV3RowLineage
        extends TestIcebergRowLineageBase
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    @DataProvider(name = "pushdownFilterEnabled")
    public Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Override
    @DataProvider(name = "pushdownFilterEnabledAndBoundlessLineageMetricsMode")
    public Object[][] pushdownFilterEnabledAndBoundlessLineageMetricsModeProvider()
    {
        return new Object[][] {
                {true, "counts"},
                {false, "counts"},
                {true, "none"},
                {false, "none"}
        };
    }

    /**
     * Compares {@code sql} on the native workers under {@code session} with the Java workers, which
     * always run with pushdown disabled.
     */
    @Override
    protected void assertMatchesExpectedEngine(Session session, String sql)
    {
        assertQuery(session, sql, sessionWithPushdown(false), sql);
    }

    @Override
    protected File getCatalogDirectory()
    {
        return IcebergQueryRunner.getIcebergDataDirectoryPath(
                        getDistributedQueryRunner().getCoordinator().getDataDirectory(),
                        CatalogType.HADOOP.name(),
                        new IcebergConfig().getFileFormat(),
                        true)
                .toFile();
    }
}
